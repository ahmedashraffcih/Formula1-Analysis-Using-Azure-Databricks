# Databricks notebook source
# MAGIC %run "./includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

spark.sql("SELECT COUNT(1) FROM v_race_results WHERE race_year = 2020").show()

# COMMAND ----------

race_results_df.("gv_race_results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.v1_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE()

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.write.format("parquet").saveAsTable("demo.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE race_results_sql
# MAGIC AS
# MAGIC SELECT * FROM demo.race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CURRENT_DATABASE()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo

# COMMAND ----------

race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_external").saveAsTable("demo.race_results_external")

# COMMAND ----------


