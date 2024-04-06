# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors Single-Line JSON File

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read JSON file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
.schema(schema)\
.json('dbfs:/mnt/forumla1dl/raw/constructors.json')

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename Column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                                .withColumnRenamed("constructorRef","constructor_ref")\
                                                .withColumn("ingestion_date",current_timestamp())

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/forumla1dl/processed/constructors")
display(spark.read.parquet("/mnt/forumla1dl/processed/constructors"))
