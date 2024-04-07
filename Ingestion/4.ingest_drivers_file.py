# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Drivers JSON File

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

name_schema = StructType([StructField('forename', StringType(), True),
                          StructField('surname', StringType(), True)
                          ])
schema = StructType([StructField('driverId', IntegerType(), False), 
                     StructField('driverRef', StringType(), True), 
                     StructField('number', IntegerType(), True), 
                     StructField('code', StringType(), True), 
                     StructField('name', name_schema),
                     StructField('dob', DateType(), True), 
                     StructField('nationality', StringType(), True), 
                     StructField('url', StringType(), True)
                     ])

# COMMAND ----------

drivers_df = spark.read\
.schema(schema)\
.json(f"{raw_folder_path}/drivers.json")

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. Ingestion date added
# MAGIC 4. name added with concatenation for forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId","driver_id")\
                                                .withColumnRenamed("driverRef","driver_ref")\
                                                .withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))\
                                                .withColumn("data_source", lit(v_data_source))

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns
# MAGIC 1. name.forname
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop('url')

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
display(spark.read.parquet(f"{processed_folder_path}/drivers"))
