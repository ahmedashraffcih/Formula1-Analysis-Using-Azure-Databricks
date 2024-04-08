# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results Single-Line JSON File

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read JSON file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

# COMMAND ----------

schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True),
                            StructField("positionOrder", IntegerType(), True),
                            StructField("points", FloatType(), True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            StructField("fastestLap", IntegerType(), True),
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", FloatType(), True),
                            StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read\
.schema(schema)\
.json(f"{raw_folder_path}/results.json")

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id")\
                                .withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("constructorId","constructor_id")\
                                .withColumnRenamed("positionText","position_text")\
                                .withColumnRenamed("positionOrder","position_order")\
                                .withColumnRenamed("fastestLap","fastest_lap")\
                                .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                .withColumn("data_source", lit(v_data_source))

results_with_ingestion_date_df = add_ingestion_date(results_renamed_df)

display(results_with_ingestion_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop('statusId')

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")
display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")