# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Pit Stops Multi-Line JSON File
# MAGIC %md
# MAGIC ##### Step 1 - Read JSON file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

# COMMAND ----------

schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                            StructField("driverId", IntegerType(), True),
                            StructField("stop", StringType(), True),
                            StructField("lap", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("duration", StringType(), True),
                            StructField("milliseconds", IntegerType(), True)
                            ])

# COMMAND ----------

pit_stops_df = spark.read\
.schema(schema)\
.option("multiLine", True) \
.json('dbfs:/mnt/forumla1dl/raw/pit_stops.json')

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("ingestion_date",current_timestamp())

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/forumla1dl/processed/pit_stops")
display(spark.read.parquet("/mnt/forumla1dl/processed/pit_stops"))
