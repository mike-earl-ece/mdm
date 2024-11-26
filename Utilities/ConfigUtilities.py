# Databricks notebook source
# MAGIC %md
# MAGIC # Config Utilities
# MAGIC Functions that suppport reuse of key configuration information.

# COMMAND ----------

# Constants
CONTAINER_URI_PATH = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"
MDM_CLEANED_PATH = CONTAINER_URI_PATH + "SilverConformed/MDM/Cleaned/"
MDM_INGEST_TABLE = "default.bronze_mdmingest"
MDM_CLEANED_TABLE = "default.silverconformed_mdm_cleaned"

# COMMAND ----------

def set_spark_config():
    storage_end_point = "ecemdmstore.dfs.core.windows.net" 
    my_scope = "MeterDataStorageVault"
    my_key = "MeterStorageAccessKey"

    spark.conf.set(
        "fs.azure.account.key." + storage_end_point,
        dbutils.secrets.get(scope=my_scope, key=my_key))

# COMMAND ----------

def hello_world():
    return("Hello World")
