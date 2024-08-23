# Databricks notebook source
# Databricks notebook source
storage_end_point = "ecemdmstore.dfs.core.windows.net" 
my_scope = "MeterDataStorageVault"
my_key = "MeterStorageAccessKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"
checkpoint_path = uri + "Bronze/Checkpoint"

input_path = uri + "Bronze/"

table_name = "MeterData3"


# COMMAND ----------

dbutils.fs.ls(input_path)

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

df1 = spark.readStream.format("cloudFiles") \
    .schema(schema) \
    .option("cloudFiles.format", "csv") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.schemaLocation", checkpoint_path ) \
    .load(input_path) 

query = df1.writeStream \
    .format("delta") \
    .option("checkpointLocation", "Checkpoint") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .table(table_name)

query.awaitTermination()



# COMMAND ----------

from pyspark.sql.functions import min, max

df2 = spark.read.table(table_name)

df2_cleaned = df2.filter(df2.MeterNumber.isNotNull())

print(df2_cleaned.count())

display(df2_cleaned.agg(min('StartDateTime').alias('FirstSampleTime'), max('EndDateTime').alias('LastSampleTime')))
