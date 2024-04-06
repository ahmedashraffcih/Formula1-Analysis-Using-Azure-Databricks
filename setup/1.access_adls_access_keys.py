# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

f1_key = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.forumla1dl.dfs.core.windows.net",
    f1_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


