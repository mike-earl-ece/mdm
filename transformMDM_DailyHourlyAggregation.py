# Databricks notebook source
# MAGIC %md
# MAGIC # Transform MDM Daily Hourly Aggregation
# MAGIC Aggregate data to the day and hour ending across all data points for both AMI and VEE values.  This will enable heat maps and other Power BI reporting.

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import min, max, sum, year, month, hour, day

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

output_path = uri + "Gold/MeterMetrics/DailyHourlyAggregation"

# COMMAND ----------

# Read the data from the source table
meter_df_cleaned = spark.read.format("delta").load(uri + "Silver/Conformed")

if debug:
    display(meter_df_cleaned)


# COMMAND ----------

# Aggregate to hourly for each day.
meter_df_cleaned = meter_df_cleaned.withColumn("Year", year(meter_df_cleaned.EndDateTime))\
                            .withColumn("Month", month(meter_df_cleaned.EndDateTime))\
                            .withColumn("Day", day(meter_df_cleaned.EndDateTime))\
                            .withColumn("HourEnding", hour(meter_df_cleaned.EndDateTime))

daily_hourly_df = meter_df_cleaned.groupBy("Year", "Month", "Day", "HourEnding").agg(sum("AMIValue").alias("TotalAMIValue"), 
                                                                                     sum("VEEValue").alias("TotalVEEValue"))

if debug:
    display(daily_hourly_df)

# COMMAND ----------

# Check on count by month.
if debug:
    display(meter_df_cleaned.groupBy("Year", "Month").count())

# COMMAND ----------

# Write out to delta format.
daily_hourly_df.write.mode('overwrite').format("delta").save(output_path)

# Clean up old files
delta_table = DeltaTable.forPath(spark, output_path)
delta_table.vacuum()  
