# Databricks notebook source
# MAGIC %md
# MAGIC # Create Meter Serial Index Calendar
# MAGIC Creates a "calendar" with an index every 5 minutes.  The goal is to join this index with meter data and other inputs such as load control periods, coincident load, etc to simply joins vs. using a timestamp or timestamp range.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

# Create an index for n years.
import pandas as pd
from datetime import datetime, time, timedelta
from pyspark.sql.functions import year, month, day, hour, minute, col, monotonically_increasing_id


start_year = 2023
end_year = 2030

# COMMAND ----------

# Create a timestamp for every 5 minutes for the year range.
def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

#TimeStampStr = [dt.strftime('%Y-%m-%d T%H:%M Z') for dt in 
#       datetime_range(datetime(start_year, 1, 1, 0), datetime(end_year, 12, 31, 23, 59), 
#       timedelta(minutes=5))]

TimeStamp = [dt for dt in 
       datetime_range(datetime(start_year, 1, 1, 0), datetime(end_year, 12, 31, 23, 59), 
       timedelta(minutes=5))]

# COMMAND ----------

print(type(TimeStamp))
print(type(TimeStamp[0]))


# COMMAND ----------

# Create a PySpark dataframe with the timestamp.
timestamps_df = spark.createDataFrame(pd.DataFrame(TimeStamp, columns=["TimeStamp"]))
display(timestamps_df)

# COMMAND ----------

# Create new columns with information extracted from the timestamp.
timestamps_df = timestamps_df.withColumn("Year", year(col("TimeStamp"))) \
                                    .withColumn("Month", month(col("TimeStamp"))) \
                                    .withColumn("Day", day(col("TimeStamp"))) \
                                    .withColumn("Hour", hour(col("TimeStamp"))) \
                                    .withColumn("Minute", minute(col("TimeStamp")))

display(timestamps_df)

# COMMAND ----------

# Add the index.
timestamps_df = timestamps_df.withColumn("MeterSampleIndex", monotonically_increasing_id())

display(timestamps_df)

# COMMAND ----------

max_index_row = timestamps_df.orderBy(col("MeterSampleIndex").desc()).limit(1)
display(max_index_row)

# COMMAND ----------

# Save in the SilverConformed area of storage.

# Save the data as Parquet format.
timestamps_df.write.option("header",True).mode('overwrite').parquet(INDEXED_CALENDAR_PATH)   
