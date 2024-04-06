# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using SAS Token
# MAGIC 1. Set the spark config SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.forumla1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.forumla1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.forumla1dl.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1dl.dfs.core.windows.net/circuits.csv"))
