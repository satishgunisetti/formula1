# Databricks notebook source
storage_account_name = "formula1dlepam"
client_id = dbutils.secrets.get("formula1-scope", "databricks-app-client-id")
tenant_id = dbutils.secrets.get("formula1-scope", "databricks-app-tenant-id")
client_secret  = dbutils.secrets.get("formula1-scope", "databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

configs

# COMMAND ----------

def mount_adfs(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

container_name = "processed"

mount_adfs(container_name)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/{storage_account_name}/processed")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1dlepam/raw'))

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls('/mnt/formula1dlepam/processed')
