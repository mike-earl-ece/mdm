# Databricks notebook source
debug = 1

# COMMAND ----------

storage_end_point = "ecemdmstore.dfs.core.windows.net" 
my_scope = "MeterDataStorageVault"
my_key = "MeterStorageAccessKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"
checkpoint_path = uri + "Bronze/Checkpoint"

input_path = uri + "Bronze/"
output_path = uri + "Bronze6/Output"

table_name = "MeterData3"


# COMMAND ----------


if debug:
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
    .option("checkpointLocation", checkpoint_path) \
    .option("path", output_path) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .table(table_name)    

query.awaitTermination()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Data cleaning
# MAGIC The following section takes the following actions to prep the data for further processing:
# MAGIC - Remove the header row that got ingested from each file.
# MAGIC - Removes the _rescued_data column.  This column has information on the ingestion source for each data point.
# MAGIC - Drops duplicate rows to handle the situation when the same data points are ingested multiple times.
# MAGIC
# MAGIC The resulting data set is saved to the Silver folder in delta format.

# COMMAND ----------

from pyspark.sql.functions import min, max

df2 = spark.read.table(table_name)

if debug:
    print(df2.count())

df2_cleaned = df2.filter(df2.MeterNumber.isNotNull())
df2_cleaned = df2_cleaned.drop('_rescued_data')
df2_cleaned = df2_cleaned.dropDuplicates()

if debug:
    print(df2_cleaned.count())
    display(df2_cleaned.agg(min('StartDateTime').alias('FirstSampleTime'), max('EndDateTime').alias('LastSampleTime')))

# COMMAND ----------

# Save the data to the silver tier.  
# ToDo:  investigate update patterns as an alternative to overwrite.
df2_cleaned.write.format("delta").mode("overwrite").save(uri + "Silver/Conformed")
