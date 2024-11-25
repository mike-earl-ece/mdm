# Databricks notebook source
# MAGIC %md
# MAGIC # Clean MDM
# MAGIC Cleans MDM data ingested into the Bronze tier. 

# COMMAND ----------

# Imports and debug
debug = 1

# COMMAND ----------

# Set spark config
storage_end_point = "ecemdmstore.dfs.core.windows.net" 
my_scope = "MeterDataStorageVault"
my_key = "MeterStorageAccessKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))


# COMMAND ----------

# Initialize key variables
uri = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"

checkpoint_path = uri + "SilverConformed/MDM/Cleaned/Checkpoint"
input_table_name = "default.bronze_mdmingest"
output_table_name = "default.silver_mdmcleaned"
