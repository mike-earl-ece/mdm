# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Bronze
# MAGIC This notebook creates the required table for the bronze tier.  This should only need to be executed once.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

debug=1

# COMMAND ----------

set_spark_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up code
# MAGIC The following cells delete the table if needed.  This should only be executed if needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.bronze_mdmingest
# MAGIC
# MAGIC -- Manually clean up the data at MDM_INGEST_OUTPUT_PATH and MDM_INGEST_CHECKPOINT_PATH

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the bronze table.  This should be created as an external table so we can explicitly manage the storage.  
# MAGIC -- Note that creating the table in storage first has issues with enabling change data feed.
# MAGIC CREATE TABLE IF NOT EXISTS default.bronze_mdmingest 
# MAGIC USING DELTA LOCATION "abfss://meter-data@ecemdmstore.dfs.core.windows.net/Bronze/MDM"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
