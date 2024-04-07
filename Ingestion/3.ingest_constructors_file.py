# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors Single-Line JSON File

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
.schema(schema)\
.json(f"{raw_folder_path}/constructors.json")

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

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                                .withColumnRenamed("constructorRef","constructor_ref")\
                                                .withColumn("data_source", lit(v_data_source))

constructors_final_df = add_ingestion_date(constructors_renamed_df)
display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
display(spark.read.parquet(f"{processed_folder_path}/constructors"))
