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

print(CONTAINER_URI_PATH)

# COMMAND ----------

# Look at what gets extracted for a single day.
df = spark.read.format("csv").option("header", True).schema(schema).load("abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/" + "TestData/dfroslie_test_single_day-20250101-20250102-1737149538905.csv", schema=schema, header=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.format("csv").option("header", True).option("compression", "gzip").schema(schema).load(CONTAINER_URI_PATH + "MDMLandingZone/backfill-20241029-20241115-1738352817373.csv.gz", schema=schema, header=True)

dup_issue_df = df.filter((col('MeterNumber')==16116741) & (col('StartDateTime') == '2024-11-03T06:00:00.000+00:00'))
display(dup_issue_df)

dup_issue2_df = df.filter((col('MeterNumber')==16116744) & (col('StartDateTime') == '2024-11-03T06:00:00.000+00:00'))
display(dup_issue2_df)



# COMMAND ----------

from pyspark.sql.functions import col
dup_issue_df = df.filter((col('MeterNumber')==16116741) & (col('StartDateTime') == '2024-11-03T06:00:00.000+00:00'))

display(dup_issue_df)

# COMMAND ----------

from pyspark.sql.functions import col

aug_changes_df = df.filter(col('EndDateTime') < '2024-12-01')
display(aug_changes_df)
print(df.count())
print(aug_changes_df.count())

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
