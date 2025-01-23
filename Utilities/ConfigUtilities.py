# Databricks notebook source
# MAGIC %md
# MAGIC ### Config Utilities
# MAGIC Functions and constants that suppport reuse of key configuration information.

# COMMAND ----------

# Constants

# Storage
#CONTAINER_URI_PATH = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"  # Production
CONTAINER_URI_PATH = "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/"  # Test

# Schema - used for pathing below.
#SCHEMA = "default"  # Production
SCHEMA = "test"   # Test

# Ingestion
MDM_LANDING_PATH = CONTAINER_URI_PATH + "MDMLandingZone" 
MDM_INGEST_CHECKPOINT_PATH = CONTAINER_URI_PATH + "Bronze/Checkpoint"
MDM_INGEST_TABLE = SCHEMA + ".bronze_mdmingest"

# Bronze to SilverConformed
MDM_CLEANED_PATH = CONTAINER_URI_PATH + "SilverConformed/MDM/Cleaned/"
MDM_CLEANED_TABLE = SCHEMA + ".silverconformed_mdm_cleaned"
INDEXED_CALENDAR_PATH = CONTAINER_URI_PATH + "SilverConformed/IndexedCalendar/"
MDM_INDEXED_PATH = CONTAINER_URI_PATH + "SilverConformed/MDM/Indexed/"
MDM_INDEXED_TABLE = SCHEMA + ".silverconformed_mdm_indexed"

# Silver to Gold
MDM_DIAGNOSTICS_PATH = CONTAINER_URI_PATH + "Gold/MDM/Diagnostics/"


# COMMAND ----------

def set_spark_config():
    storage_end_point = "ecemdmstore.dfs.core.windows.net" 
    my_scope = "MeterDataStorageVault"
    my_key = "MeterStorageAccessKey"

    spark.conf.set(
        "fs.azure.account.key." + storage_end_point,
        dbutils.secrets.get(scope=my_scope, key=my_key))
