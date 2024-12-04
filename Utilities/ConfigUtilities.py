# Databricks notebook source
# MAGIC %md
# MAGIC ### Config Utilities
# MAGIC Functions and constants that suppport reuse of key configuration information.

# COMMAND ----------

# Constants

# Storage
CONTAINER_URI_PATH = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"

# Ingestion
MDM_LANDING_PATH = CONTAINER_URI_PATH + "MDMLandingZone" 
MDM_INGEST_CHECKPOINT_PATH = CONTAINER_URI_PATH + "Bronze/Checkpoint"
MDM_INGEST_TABLE = "default.bronze_mdmingest"

# Bronze to SilverConformed
MDM_CLEANED_PATH = CONTAINER_URI_PATH + "SilverConformed/MDM/Cleaned/"
MDM_CLEANED_TABLE = "default.silverconformed_mdm_cleaned"
INDEXED_CALENDAR_PATH = CONTAINER_URI_PATH + "SilverConformed/IndexedCalendar/"


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
