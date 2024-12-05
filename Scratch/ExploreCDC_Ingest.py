# Databricks notebook source
# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

set_spark_config()

# COMMAND ----------

uri = "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/"
ingest_path = uri + "MDMLandingZone"
mock_ingest_table = "default.bronze_mock_mdmingest"
checkpoint_path = uri + "Bronze_Mock/MDM/Checkpoint"

# COMMAND ----------

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


ingest_df = spark.read.table(mock_ingest_table)

print(ingest_df.count())

# COMMAND ----------

df1 = spark.readStream.format("cloudFiles") \
    .schema(schema) \
    .option("cloudFiles.format", "csv") \
    .option("header", "false") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.schemaLocation", checkpoint_path ) \
    .option("skipRows", 1) \
    .load(ingest_path) 

query = df1.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .option("mergeSchema", "True") \
    .trigger(availableNow=True) \
    .table(mock_ingest_table)    

query.awaitTermination()



# COMMAND ----------

ingest_out_df = spark.read.table(mock_ingest_table)

print(ingest_out_df.count())

display(ingest_out_df)

# COMMAND ----------

display(ingest_out_df.filter(ingest_out_df.MeterNumber.isNull()))

# COMMAND ----------

from pyspark.sql.functions import col

display(ingest_df.filter(col('MeterNumber')==37943913))
