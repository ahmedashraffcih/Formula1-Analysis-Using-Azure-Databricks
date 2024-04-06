# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Cluster Scoped Credentials
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1dl.dfs.core.windows.net/circuits.csv"))
