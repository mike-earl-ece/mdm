# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest MDM To Bronze
# MAGIC This notebook uses a streaming approach to incrementally ingest data from the MDM extract ASCII format into a delta table, maintaining the raw format.

# COMMAND ----------

# Imports and debug
debug = 1


# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

set_spark_config()

# COMMAND ----------

# Initialize key variables
uri = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"
checkpoint_path = uri + "Bronze/Checkpoint"
input_path = uri + "MDMLandingZone/*.csv" 
table_name = "default.bronze_mdmingest"
output_path = uri + "Bronze/MDM"


# COMMAND ----------

# Create the data schema to use with the ingested data
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

schema = StructType([
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

if debug:
  pre_update_df = spark.read.table(table_name)
  print("Before update count: " + str(pre_update_df.count()))


# COMMAND ----------

# Incrementally ingest the data 
df1 = spark.readStream.format("cloudFiles") \
    .schema(schema) \
    .option("cloudFiles.format", "csv") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.schemaLocation", checkpoint_path ) \
    .load(input_path) 

query = df1.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("path", output_path) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .table(table_name)    

query.awaitTermination()



# COMMAND ----------

if debug:
  post_update_df = spark.read.table(table_name)
  print("After update count: " + str(post_update_df.count()))
