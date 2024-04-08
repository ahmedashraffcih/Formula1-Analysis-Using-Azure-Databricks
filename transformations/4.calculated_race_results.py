# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
            (
                race_year INT,
                team_name STRING,
                driver_id INT,
                driver_name STRING,
                race_id INT,
                position INT,
                points INT,
                calculated_points INT,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
            )
            USING DELTA
        """)

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW race_result_updated
            AS
            SELECT 
                rc.race_year,
                c.name team_name,
                d.driver_id,
                d.name driver_name,
                r.race_id,
                r.position,
                r.points,
                11 - r.position calculated_points
            FROM f1_processed.results r 
            JOIN f1_processed.drivers d using(driver_id)
            JOIN f1_processed.constructors c using(constructor_id)
            JOIN f1_processed.races rc using(race_id)
            WHERE r.position <=10 AND r.file_date = '{v_file_date}'
        """)

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_result_updated WHERE race_year = 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results

# COMMAND ----------

# %sql
# CREATE TABLE f1_presentation.calculated_race_results
# using parquet
# AS
# SELECT 
#   rc.race_year,
#   c.name team_name,
#   d.name driver_name,
#   r.position,
#   r.points,
#   11 - r.position calculated_points
# FROM f1_processed.results r 
# JOIN f1_processed.drivers d using(driver_id)
# JOIN f1_processed.constructors c using(constructor_id)
# JOIN f1_processed.races rc using(race_id)
# WHERE r.position <=10
