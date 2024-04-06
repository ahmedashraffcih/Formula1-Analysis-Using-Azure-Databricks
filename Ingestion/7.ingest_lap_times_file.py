# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

# COMMAND ----------

schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                            StructField("driverId", IntegerType(), True),
                            StructField("lap", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("milliseconds", IntegerType(), True)
                            ])

# COMMAND ----------

lap_times_df = spark.read\
.schema(schema)\
.csv('dbfs:/mnt/forumla1dl/raw/lap_times')

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("ingestion_date",current_timestamp())

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/forumla1dl/processed/lap_times")
display(spark.read.parquet("/mnt/forumla1dl/processed/lap_times"))
