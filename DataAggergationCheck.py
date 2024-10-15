# Databricks notebook source
from pyspark.sql.functions import year, month, dayofmonth, count

# account connection and initialize variables
storage_end_point = "ecemdmstore.dfs.core.windows.net"
my_scope = "MeterDataStorageVault"
my_key = "MeterStorageAccessKey"

spark.conf.set("fs.azure.account.key." + storage_end_point, dbutils.secrets.get(scope=my_scope, key=my_key))

#ource table
source_table_name = "MeterData3"

# Read data
meter_df = spark.read.table(source_table_name)

# Filter out invalid rows and add Year, Month, Day columns for aggregation
meter_df_cleaned = meter_df.filter(meter_df.MeterNumber.isNotNull()) \
                           .withColumn("Year", year(meter_df.StartDateTime)) \
                           .withColumn("Month", month(meter_df.StartDateTime)) \
                           .withColumn("Day", dayofmonth(meter_df.StartDateTime))

# Count total data points per day
daily_data_points = meter_df_cleaned.groupBy("Year", "Month", "Day").agg(count('*').alias("TotalDataPointsPerDay"))
print("Total data points per day:")
daily_data_points.show()

#Count data points per substation per day
substation_daily_data_points = meter_df_cleaned.groupBy("Year", "Month", "Day", "SubstationCode") \
                                               .agg(count('*').alias("TotalDataPointsPerSubstationPerDay"))
print("Data points per substation per day:")
substation_daily_data_points.show()

#Verify no meter has more than one entry per exact timestamp
duplicate_check = meter_df_cleaned.groupBy("Year", "Month", "Day", "MeterNumber", "StartDateTime") \
                                  .agg(count('*').alias("EntriesPerTimestamp"))

# Filter for any duplicates (EntriesPerTimestamp > 1)
duplicate_entries = duplicate_check.filter(duplicate_check.EntriesPerTimestamp > 1)
print(" Meters with more than one identical timestamp:")
duplicate_entries.show()


