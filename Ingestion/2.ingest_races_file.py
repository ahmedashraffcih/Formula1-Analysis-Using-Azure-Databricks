# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races CSV File

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

schema = StructType(fields=[StructField('raceId',IntegerType(), False),
                            StructField('year',IntegerType(), True),
                            StructField('round',IntegerType(), True),
                            StructField('circuitId',IntegerType(), True),
                            StructField('name',StringType(), True),
                            StructField('date',DateType(), True),
                            StructField('time',StringType(), True),
                            StructField('url',StringType(), True),
                            ])

# COMMAND ----------

races_df = spark.read\
.option('header',True)\
.schema(schema)\
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

races_df.show(5,truncate=False)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp",to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn("data_source", lit(v_data_source))

display(races_with_timestamp_df)

# COMMAND ----------

races_with_timestamp_df2 = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df2.select(col("raceId").alias("race_id"), col("year").alias("race_year"), 
                                                   col("round"), col("circuitId").alias("circuit_id"), col("name"), 
                                                   col("ingestion_date"), col("race_timestamp"))

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as partitioned parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")
display(spark.read.parquet("/mnt/forumla1dl/processed/races"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/forumla1dl/processed/races

# COMMAND ----------

dbutils.notebook.exit("Success")