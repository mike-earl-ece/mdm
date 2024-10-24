# Databricks notebook source
# MAGIC %md
# MAGIC # Transform MDM Hourly Aggregation
# MAGIC Aggregate data to the hour ending across all data points for both AMI and VEE values.  Calculate AMI vs. VEE differences for each hour.

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import min, max, sum, hour

debug=1

# COMMAND ----------

# Set up storage account connection and initialize variables.
storage_end_point = "ecemdmstore.dfs.core.windows.net" 
my_scope = "MeterDataStorageVault"
my_key = "MeterStorageAccessKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"

output_path = uri + "Gold/MeterMetrics/HourlyAggregation"


# COMMAND ----------

# Read the data from the source table
meter_df_cleaned = spark.read.format("delta").load(uri + "Silver/Conformed")

if debug:
    display(meter_df_cleaned)


# COMMAND ----------

# Aggregate to hourly
meter_df_cleaned = meter_df_cleaned.withColumn("HourEnding", hour(meter_df_cleaned.EndDateTime))

hourly_df = meter_df_cleaned.groupBy("HourEnding").agg(sum("AMIValue").alias("TotalAMIValue"), sum("VEEValue").alias("TotalVEEValue"))

hourly_df = hourly_df.withColumn("AMI_VEE_kWhDifference", (hourly_df.TotalVEEValue - hourly_df.TotalAMIValue))
hourly_df = hourly_df.withColumn("AMI_VEE_PercentDifference", (hourly_df.TotalVEEValue - hourly_df.TotalAMIValue) / hourly_df.TotalAMIValue)

if debug:
    display(hourly_df)



# COMMAND ----------

# Write out to delta format.
hourly_df.write.mode('overwrite').format("delta").save(output_path)

# Clean up old files
delta_table = DeltaTable.forPath(spark, output_path)
delta_table.vacuum()  
