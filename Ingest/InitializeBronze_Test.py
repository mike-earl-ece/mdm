# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Bronze Test
# MAGIC This notebook creates the required table for the bronze tier in the Test container.  This should only need to be executed once.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

set_spark_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up code
# MAGIC The following cells delete the table if needed.  This should only be executed if needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE test.bronze_mdmingest
# MAGIC
# MAGIC -- Manually clean up the data at MDM_INGEST_OUTPUT_PATH and MDM_INGEST_CHECKPOINT_PATH

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the schema for test. Production uses the default schema.  This assumes legacy Hive and not Unity.
# MAGIC CREATE SCHEMA IF NOT EXISTS test

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the bronze table.  This should be created as an external table so we can explicitly manage the storage.  
# MAGIC -- Note that creating the table in storage first has issues with enabling change data feed.
# MAGIC CREATE TABLE IF NOT EXISTS test.bronze_mdmingest 
# MAGIC USING DELTA LOCATION "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/Bronze/MDM"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
