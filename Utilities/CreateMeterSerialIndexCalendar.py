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

# Print to ensure we're pointing to the test storage account.
print(INDEXED_CALENDAR_PATH)

# COMMAND ----------

# Create an index for n years.
import pandas as pd
from datetime import datetime, time, timedelta
from pyspark.sql.functions import year, month, day, hour, minute, col, monotonically_increasing_id, from_utc_timestamp, when, lit, dayofweek, udf
from pyspark.sql.types import IntegerType

start_year = 2021
end_year = 2040

# COMMAND ----------

# Install a holidays library
%pip install holidays

import holidays

# COMMAND ----------

# Create a timestamp for every 5 minutes for the year range.
def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

TimeStamp = [dt for dt in 
       datetime_range(datetime(start_year, 1, 1, 0), datetime(end_year, 12, 31, 23, 59), 
       timedelta(minutes=5))]

# COMMAND ----------

# Create a PySpark dataframe with the timestamp.
timestamps_df = spark.createDataFrame(pd.DataFrame(TimeStamp, columns=["UTCTimeStamp"]))
display(timestamps_df)

# COMMAND ----------

# Create new columns with information extracted from the timestamp.
timestamps_df = timestamps_df.withColumn("UTCYear", year(col("UTCTimeStamp"))) \
                                    .withColumn("UTCMonth", month(col("UTCTimeStamp"))) \
                                    .withColumn("UTCDay", day(col("UTCTimeStamp"))) \
                                    .withColumn("UTCHour", hour(col("UTCTimeStamp"))) \
                                    .withColumn("UTCMinute", minute(col("UTCTimeStamp")))

display(timestamps_df)

# COMMAND ----------

# Add a local time timestamp and detail.
timestamps_df = timestamps_df.withColumn("LocalTimeStamp", from_utc_timestamp(col("UTCTimeStamp"), "America/Chicago")) \
                                    .withColumn("LocalYear", year(col("LocalTimeStamp"))) \
                                    .withColumn("LocalMonth", month(col("LocalTimeStamp"))) \
                                    .withColumn("LocalDay", day(col("LocalTimeStamp"))) \
                                    .withColumn("LocalHour", hour(col("LocalTimeStamp"))) \
                                    .withColumn("LocalMinute", minute(col("LocalTimeStamp")))

display(timestamps_df)

# COMMAND ----------

# Add the index.
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("UTCTimeStamp")
timestamps_df = timestamps_df.withColumn("MeterSampleIndex", row_number().over(window_spec))

display(timestamps_df)

# COMMAND ----------

max_index_row = timestamps_df.orderBy(col("MeterSampleIndex").desc()).limit(1)
display(max_index_row)

# COMMAND ----------

# Add weekend and holiday flags.

# Define a function to check if a date is a holiday
def is_holiday(date):
    us_holidays = holidays.US(years=date.year)
    return date in us_holidays

# Add weekend column
timestamps_holiday_df = timestamps_df.withColumn('Weekend', when(dayofweek(col('LocalTimeStamp')).isin([1, 7]), lit(1)).otherwise(lit(0)))

# Add holiday column
is_holiday_udf = udf(lambda date: 1 if is_holiday(date) else 0, IntegerType())
timestamps_holiday_df = timestamps_holiday_df.withColumn('Holiday', is_holiday_udf(col('LocalTimeStamp')))

display(timestamps_holiday_df)

# COMMAND ----------

# Save in the SilverConformed area of storage.

# Save the data as Parquet format.
timestamps_df.write.option("header",True).mode('overwrite').parquet(INDEXED_CALENDAR_PATH)   
