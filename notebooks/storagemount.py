# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "da479f2b-d477-4947-a548-f182c5681fde",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="tennis-scope", key="tennis-databricks-key-vault-secret"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ec527a93-8f1a-4c48-8dfb-b9c10c17ec50/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://tennis-container@guidosaccount.dfs.core.windows.net/bronze",
  mount_point = "/mnt/bronze",
  extra_configs = configs,
)

dbutils.fs.mount(
  source = "abfss://tennis-container@guidosaccount.dfs.core.windows.net/silver",
  mount_point = "/mnt/silver",
  extra_configs = configs,
)

dbutils.fs.mount(
  source = "abfss://tennis-container@guidosaccount.dfs.core.windows.net/gold",
  mount_point = "/mnt/gold",
  extra_configs = configs,
)

# COMMAND ----------

dbutils.fs.ls("/mnt/")

# COMMAND ----------

dbutils.fs.ls("/mnt/")

# COMMAND ----------



# COMMAND ----------

dbutils.