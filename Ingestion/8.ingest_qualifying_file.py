# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

# COMMAND ----------

schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("q1", StringType(), True),
                            StructField("q2", StringType(), True),
                            StructField("q3", StringType(), True),
                            ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(schema)\
.option("multiLine", True) \
.json('/mnt/forumla1dl/raw/qualifying')

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
                                .withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("constructorId","constructor_id")\
                                .withColumn("ingestion_date",current_timestamp())

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/forumla1dl/processed/qualifying")
display(spark.read.parquet("/mnt/forumla1dl/processed/qualifying"))
