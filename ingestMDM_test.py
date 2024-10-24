# Databricks notebook source
storage_end_point = "ecemdmstore.dfs.core.windows.net" 
my_scope = "MeterDataStorageVault"
my_key = "MeterStorageAccessKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/"
checkpoint_path = uri + "Output/_checkpoint"

input_path = uri + "Input/"
output_path = uri + "Output"

table_name = "MeterData999"


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



# COMMAND ----------

#spark.sql(f"CREATE TABLE IF NOT EXISTS default.{table_name} USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS default.meterdata999 USING DELTA")


# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, table_name)

#deltaTable = DeltaTable.forName(spark, "default.meterdata3")


# COMMAND ----------


# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  (deltaTable.alias("t").merge(
      microBatchOutputDF.alias("s"),
      "s.key = t.key")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
  )

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
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .trigger(availableNow=True) \
    .table(table_name)    

query.awaitTermination()


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

from pyspark.sql.functions import min, max

df2 = spark.read.table(table_name)

df2_cleaned = df2.filter(df2.MeterNumber.isNotNull())

print(df2_cleaned.count())

display(df2_cleaned.agg(min('StartDateTime').alias('FirstSampleTime'), max('EndDateTime').alias('LastSampleTime')))

display(df2_cleaned)

# COMMAND ----------

df2_cleaned_nometadata = df2_cleaned.drop("_rescued_data")
df2_duplicates = df2_cleaned_nometadata.groupBy(df2_cleaned_nometadata.columns).count().filter("count > 1").drop("count")

print(df2_duplicates.count())
display(df2_duplicates)

# COMMAND ----------

df2_cleaned = df2_cleaned.dro
