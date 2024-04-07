# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits CSV File

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read CSV file using spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/forumla1dl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

schema = StructType(fields=[StructField('circuitId',IntegerType(), False),
                            StructField('circuitRef',StringType(), True),
                            StructField('name',StringType(), True),
                            StructField('location',StringType(), True),
                            StructField('country',StringType(), True),
                            StructField('lat',DoubleType(), True),
                            StructField('lng',DoubleType(), True),
                            StructField('alt',IntegerType(), True),
                            StructField('url',StringType(), True)
                            ])

# COMMAND ----------

circuits_df = spark.read\
.option('header',True)\
.schema(schema)\
.csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

circuits_df.schema

# COMMAND ----------

circuits_df.show(5,truncate=False)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename Columns As Required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")\
    .withColumn("data_source", lit(v_data_source))

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
