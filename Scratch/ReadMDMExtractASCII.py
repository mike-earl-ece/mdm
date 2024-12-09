# Databricks notebook source
# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

set_spark_config()

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

df = spark.read.csv(CONTAINER_URI_PATH + "MDMLandingZone", schema=schema, header=True)
display(df)

# COMMAND ----------

dbutils.fs.ls(CONTAINER_URI_PATH + "MDMLandingZone")

# COMMAND ----------

# Read zip format
df = spark.read.format("csv").option("compression", "gzip").option("header", True).schema(schema).load(CONTAINER_URI_PATH + "MDMLandingZone")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

print(df.count())

display(df.groupBy(col("UnitOfMeasure")).count())
