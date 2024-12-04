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

# Create desired schema
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

ingest_schema = StructType([
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
    StructField("VEEValue", FloatType(), True),
    StructField("_rescued_data", StringType(), True)
])

# COMMAND ----------

# Create an empty dataframe and write to storage

# Note - this doesn't appear to be needed and actually causes issues.
# empty_df = spark.createDataFrame([], schema=ingest_schema)

# if debug:
#     display(empty_df)

# empty_df.write.format("delta").mode("overwrite").save("abfss://meter-data@ecemdmstore.dfs.core.windows.net/Bronze/MDM")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the bronze table.  This should be created as an external table so we can explicitly manage the storage.
# MAGIC CREATE TABLE IF NOT EXISTS default.bronze_mdmingest 
# MAGIC USING DELTA LOCATION "abfss://meter-data@ecemdmstore.dfs.core.windows.net/Bronze/MDM"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
