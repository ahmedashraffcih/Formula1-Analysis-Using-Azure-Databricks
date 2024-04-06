# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenat_id and client_secret from key vault
# MAGIC 2. Set the spark config with App/Client Id, Directory/Tenat Id & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-client-id')
tenat_id = sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-tenat-id')
client_secret = sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenat_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@forumla1dl.dfs.core.windows.net/",
  mount_point = "/mnt/forumla1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/forumla1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/forumla1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/forumla1dl/demo')
