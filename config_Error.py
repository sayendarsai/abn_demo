# Databricks notebook source


# Application/Client ID. - 458224e2-c9bb-400f-8623-34052a632750
# tenent ID. - e52d37f2-4348-4551-8938-196f78d590dd
# Secret id. - wwe8Q~pq1XI71dKsLg-hNXhRBlQDwX0kfj5Mmcta
#Access connector - /subscriptions/befc37b2-447e-4e3c-a2fd-63b5687abaad/resourceGroups/DataEng_RG/providers/Microsoft.Databricks/accessConnectors/accessforadlstodbf
# container - sourcedata
# storage-account - abndemostorage


configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": 
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "458224e2-c9bb-400f-8623-34052a632750",
  "fs.azure.account.oauth2.client.secret": "wwe8Q~pq1XI71dKsLg-hNXhRBlQDwX0kfj5Mmcta",
  "fs.azure.account.oauth2.client.endpoint": 
    "https://login.microsoftonline.com/e52d37f2-4348-4551-8938-196f78d590dd/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://sourcedata@abndemostorage.dfs.core.windows.net/",
  mount_point = "/mnt/abndemostorage/sourcedata",
  extra_configs = configs
)

# COMMAND ----------

#test