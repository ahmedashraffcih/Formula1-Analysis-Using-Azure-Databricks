# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results Single-Line JSON File

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
.json('dbfs:/mnt/forumla1dl/raw/results.json')

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

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
                                .withColumn("ingestion_date",current_timestamp())

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns

# COMMAND ----------

results_final_df = results_renamed_df.drop('statusId')

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/forumla1dl/processed/results")
display(spark.read.parquet("/mnt/forumla1dl/processed/results"))
