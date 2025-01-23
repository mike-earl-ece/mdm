# Databricks notebook source
# MAGIC %md
# MAGIC # Create Indexed Calendar
# MAGIC Creates a parquet version of the indexed calendar.

# COMMAND ----------

# Imports and debug
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, to_timestamp

debug = 1


# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

# Initialize key variables. The constants are defined in ConfigUtilities.
uri = CONTAINER_URI_PATH
output_path = INDEXED_CALENDAR_PATH
input_path = uri + "Bronze/IndexedCalendar/Meter15Calendar.csv"

if debug:
    print(input_path)
    print(output_path)

# COMMAND ----------

cal_df =  spark.read.option("header", True).csv(input_path)

if debug:
    display(cal_df) 
    print(cal_df.dtypes)

# COMMAND ----------

# Convert the types of all columns except for Time, which will remain a string.
cal_types_df = cal_df.withColumn('Date', to_date(cal_df.Date, "yyyy-MM-dd")) \
                     .withColumn('TimeStamp', to_timestamp(cal_df.TimeStamp, "yyyy-MM-dd HH:mm")) \
                     .withColumn('Year', cal_df.Year.cast(IntegerType())) \
                     .withColumn('Month', cal_df.Month.cast(IntegerType())) \
                     .withColumn('Day', cal_df.Day.cast(IntegerType())) \
                     .withColumn('Hour', cal_df.Hour.cast(IntegerType())) \
                     .withColumn('Minute', cal_df.Minute.cast(IntegerType())) \
                     .withColumn('Interval', cal_df.Interval.cast(IntegerType())) \
                     .withColumn('MeterSampleIndex', cal_df.MeterSampleIndex.cast(IntegerType()))  

if debug:
    display(cal_types_df)
    print(cal_types_df.dtypes)

# COMMAND ----------

# Save the data as Parquet format.
cal_types_df.write.option("header",True).mode('overwrite').parquet(output_path)   


# COMMAND ----------

# Read back in to confirm types and data.
if debug:
    cal_test_df =  spark.read.parquet(output_path)

    display(cal_test_df)
    print(cal_test_df.dtypes)
    print(cal_test_df.count())

