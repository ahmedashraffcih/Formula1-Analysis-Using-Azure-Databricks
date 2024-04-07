# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
.csv(f"{raw_folder_path}/lap_times")

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final_df = lap_times_with_ingestion_date_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("data_source", lit(v_data_source))

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
