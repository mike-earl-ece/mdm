# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Silver Conformed Test
# MAGIC This notebook creates the required tables for the silver conformed tier in the test container.  This commands should only need to be executed once.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities
# MAGIC

# COMMAND ----------

debug=1

set_spark_config()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop table if needed for reset
# MAGIC DROP TABLE test.silverconformed_mdm_cleaned
# MAGIC
# MAGIC -- Clean up the delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop table if needed for reset
# MAGIC DROP TABLE test.silverconformed_mdm_indexed
# MAGIC
# MAGIC -- Clean up the delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the  cleaned table.  This should be created as an external table so we can explicitly manage the storage.
# MAGIC CREATE TABLE IF NOT EXISTS test.silverconformed_mdm_cleaned 
# MAGIC USING DELTA LOCATION "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/SilverConformed/MDM/Cleaned"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the indexed table.  This should be created as an external table so we can explicitly manage the storage.
# MAGIC CREATE TABLE IF NOT EXISTS test.silverconformed_mdm_indexed 
# MAGIC USING DELTA LOCATION "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/SilverConformed/MDM/Indexed"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
