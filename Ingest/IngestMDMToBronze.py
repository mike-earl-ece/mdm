# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest MDM To Bronze
# MAGIC This notebook uses a streaming approach to incrementally ingest data from the MDM extract ASCII format into a delta table, maintaining the raw format.

# COMMAND ----------

# Imports and debug
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

debug = 1


# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

# Initialize key variables. The constants are defined in ConfigUtilities.
uri = CONTAINER_URI_PATH
checkpoint_path = MDM_INGEST_CHECKPOINT_PATH
input_path = MDM_LANDING_PATH 
table_name = MDM_INGEST_TABLE

if debug:
    print("uri: " + uri)
    print("checkpoint_path: " + checkpoint_path)
    print("input_path: " + input_path)
    print("table_name: " + table_name)


# COMMAND ----------

# Create the data schema to use with the ingested data.  Since the data is in CSV format, we need to define the schema.
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


# Get a count of rows in the Bronze table before ingestion.
if debug:
  try:
    pre_update_df = spark.read.table(table_name)
    print("Before update count: " + str(pre_update_df.count()))
  except AnalysisException as e:
    print(str(e))


# COMMAND ----------

# Incrementally ingest the data.  These streaming APIs will look for new files in the MDM Extract landing location
# and write them to the Bronze delta table.  If there is redundant data in the new files from earlier runs, this will 
# be included in the table.  The transformation from Bronze to SilverConfirmed will update the records as needed.
df1 = spark.readStream.format("cloudFiles") \
    .schema(schema) \
    .option("cloudFiles.format", "csv") \
    .option("compression", "gzip") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.schemaLocation", checkpoint_path ) \
    .option("header", "false") \
    .option("skipRows", 1) \
    .load(input_path) 

query = df1.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .table(table_name)    

query.awaitTermination()



# COMMAND ----------

# Get a count of rows in the Bronze table after ingestion.
if debug:
  post_update_df = spark.read.table(table_name)
  print("After update count: " + str(post_update_df.count()))
