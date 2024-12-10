# Databricks notebook source
# MAGIC %md
# MAGIC # Explore CDC 
# MAGIC Notebook that researches how delta table's changed data capture can be used for incremental processing.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

set_spark_config()

# COMMAND ----------

uri = "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/"
ingest_path = uri + "MDMLandingZone"
mock_ingest_table = "default.bronze_mock_mdmingest_2"
mock_cleaned_table = "default.silverconformed_mock_mdm_cleaned"

# COMMAND ----------

# Create a table for mock ingestion with CDC enabled.
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


clean_schema = StructType([
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

# Create a table for mock bronze data.  Keep CDC enabled for use with downstream processing.
# First, create an empty dataframe and write to storage
empty_df = spark.createDataFrame([], schema=ingest_schema)

display(empty_df)

empty_df.write.format("delta").mode("overwrite").save("abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/Bronze_Mock/MDM")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the bronze table from the delta lake created in the prevoius step.  
# MAGIC CREATE TABLE IF NOT EXISTS default.bronze_mock_mdmingest_2 
# MAGIC USING DELTA LOCATION "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/Bronze_Mock/MDM"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# Test read behavior
ingest_df = spark.read.table(mock_ingest_table)

print(ingest_df.count())

# COMMAND ----------

# Test changes behavior
changes_df = spark.read \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table(mock_ingest_table)

display(changes_df)


# COMMAND ----------

# Now create a table for mock cleaned data.  Keep CDC enabled for use with downstream processing.
# First, create an empty dataframe and write to storage
empty2_df = spark.createDataFrame([], schema=clean_schema)

display(empty2_df)

empty2_df.write.format("delta").mode("overwrite").save("abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/SilverConformed_Mock/MDM/Cleaned")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the cleaned table from the delta lake created in the prevoius step.  
# MAGIC CREATE TABLE IF NOT EXISTS default.silverconformed_mock_mdmcleaned 
# MAGIC USING DELTA LOCATION "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/SilverConformed_Mock/MDM/Cleaned"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optional clean up: delete data from the bronze table.
# MAGIC DELETE FROM default.bronze_mock_mdmingest;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.silverconformed_mock_mdmingest
