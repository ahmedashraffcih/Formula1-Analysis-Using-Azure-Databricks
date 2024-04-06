# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal 
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set the spark config with App/Client Id, Directory/Tenat Id & Secret
# MAGIC 4. Assign role 'Storage Blob Contributor' to the Data Lake

# COMMAND ----------

client_id = sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-client-id')
tenat_id = sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-tenat-id')
client_secret = sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='f1-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.forumla1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.forumla1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.forumla1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.forumla1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.forumla1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenat_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1dl.dfs.core.windows.net/circuits.csv"))
