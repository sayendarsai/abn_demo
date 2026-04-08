# Databricks notebook source
#spark.conf.set("fs.azure.account.auth.type.abndemostorage.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.abndemostorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.abndemostorage.dfs.core.windows.net", "458224e2-c9bb-400f-8623-34052a632750")
#spark.conf.set("fs.azure.account.oauth2.client.secret.abndemostorage.dfs.core.windows.net", "wwe8Q~pq1XI71dKsLg-hNXhRBlQDwX0kfj5Mmcta")
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.abndemostorage.dfs.core.windows.net", "https://login.microsoftonline.com/e52d37f2-4348-4551-8938-196f78d590dd/oauth2/token")


spark.conf.set("fs.azure.account.auth.type.abndemostorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.abndemostorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.abndemostorage.dfs.core.windows.net", "458224e2-c9bb-400f-8623-34052a632750")
spark.conf.set("fs.azure.account.oauth2.client.secret.abndemostorage.dfs.core.windows.net", "wwe8Q~pq1XI71dKsLg-hNXhRBlQDwX0kfj5Mmcta")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.abndemostorage.dfs.core.windows.net", "https://login.microsoftonline.com/e52d37f2-4348-4551-8938-196f78d590dd/oauth2/token")

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC