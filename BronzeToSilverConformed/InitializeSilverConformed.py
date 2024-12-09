# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Silver Conformed
# MAGIC This notebook creates the required tables for the silver conformed tier.  This commands should only need to be executed once.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities
# MAGIC

# COMMAND ----------

debug=1

set_spark_config()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop table if needed for reset
# MAGIC DROP TABLE default.silverconformed_mdm_cleaned
# MAGIC
# MAGIC -- Clean up the delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop table if needed for reset
# MAGIC DROP TABLE default.silverconformed_mdm_indexed
# MAGIC
# MAGIC -- Clean up the delta lake

# COMMAND ----------

# Create desired schema

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

mdm_cleaned_schema = StructType([
    StructField("MeterNumber", IntegerType(), True),
    StructField("UnitOfMeasure", StringType(), True),
    StructField("FlowDirection", StringType(), True),
    StructField("Channel", IntegerType(), True),
    StructField("ServiceLocation", StringType(), True),
    StructField("RateCode", StringType(), True),
    StructField("RateDescription", StringType(), True),
    StructField("SubstationCode", StringType(), True),
    StructField("SubstationDescription", StringType(), True),
    StructField("Feeder", IntegerType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("StartDateTime", TimestampType(), True),
    StructField("EndDateTime", TimestampType(), True),
    StructField("Multiplier", IntegerType(), True),
    StructField("AMIValue", FloatType(), True),
    StructField("VEEValue", FloatType(), True)
])

# COMMAND ----------

# Create an empty dataframe and write to storage
# Note - doesn't seem to be needed, so commented out.
# empty_df = spark.createDataFrame([], schema=mdm_cleaned_schema)

# if debug:
#     display(empty_df)

# empty_df.write.format("delta").mode("overwrite").save("abfss://meter-data@ecemdmstore.dfs.core.windows.net/SilverConformed/MDM/Cleaned")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the  cleaned table.  This should be created as an external table so we can explicitly manage the storage.
# MAGIC CREATE TABLE IF NOT EXISTS default.silverconformed_mdm_cleaned 
# MAGIC USING DELTA LOCATION "abfss://meter-data@ecemdmstore.dfs.core.windows.net/SilverConformed/MDM/Cleaned"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the indexed table.  This should be created as an external table so we can explicitly manage the storage.
# MAGIC CREATE TABLE IF NOT EXISTS default.silverconformed_mdm_indexed 
# MAGIC USING DELTA LOCATION "abfss://meter-data@ecemdmstore.dfs.core.windows.net/SilverConformed/MDM/Indexed"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
