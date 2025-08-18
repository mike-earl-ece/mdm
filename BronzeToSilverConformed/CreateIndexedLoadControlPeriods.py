# Databricks notebook source
# MAGIC %md
# MAGIC # Create Indexed Meter Data
# MAGIC Transforms the cleaned MDM data by adding index and other information from the Indexed Calendar.
# MAGIC
# MAGIC This is done incrementally by finding all chagnes in the cleaned file since the last update to the indexed file.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

from pyspark.sql.functions import col, year, month, day, hour, minute, concat_ws
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

debug = 1

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

# The load control type, start, and end times are the most important fields.
schema = StructType([
    StructField("ResourceName", StringType(), True),
    StructField("LocalName", StringType(), True),
    StructField("ReasonCode", StringType(), True),
    StructField("StartDateTime", TimestampType(), True),
    StructField("EndDateTime", TimestampType(), True),
    StructField("Note", StringType(), True)
])

# COMMAND ----------

# Read all the load control events
file_name = f"{LOAD_CONTROL_INGEST_PATH}/*/*.csv"

lc_df = spark.read.csv(file_name, header=False, schema=schema)

if debug:
    print(lc_df.count())
    display(lc_df)

# COMMAND ----------

# Drop duplicates.  In theory, there shouldn't be any.
lc_df = lc_df.dropDuplicates()

if debug:
    print(lc_df.count())

# COMMAND ----------

# Read the indexed calendar
cal_df =  spark.read.parquet(INDEXED_CALENDAR_PATH)

# Load Control data is local time zone based, so drop the UTC columns for this process.
cal_df = cal_df.drop("UTCTimeStamp", "UTCYear", "UTCMonth", "UTCDay", "UTCHour", "UTCMinute")

if debug:
    display(cal_df)

# COMMAND ----------

# Join datasets by time.  We want the meter sample index for both the start and end times.

# Add time columns to the load control data for the joins.
lc_df = lc_df.withColumn("StartYear", year(col("StartDateTime"))) \
                                    .withColumn("StartMonth", month(col("StartDateTime"))) \
                                    .withColumn("StartDay", day(col("StartDateTime"))) \
                                    .withColumn("StartHour", hour(col("StartDateTime"))) \
                                    .withColumn("StartMinute", minute(col("StartDateTime"))) \
                                    .withColumn("EndYear", year(col("EndDateTime"))) \
                                    .withColumn("EndMonth", month(col("EndDateTime"))) \
                                    .withColumn("EndDay", day(col("EndDateTime"))) \
                                    .withColumn("EndHour", hour(col("EndDateTime"))) \
                                    .withColumn("EndMinute", minute(col("EndDateTime")))

if debug:
    display(lc_df)

# COMMAND ----------

# Add the start sample index.
new_data_start_df = lc_df.join(cal_df, (lc_df.StartYear == cal_df.LocalYear) & \
                                         (lc_df.StartMonth == cal_df.LocalMonth) & \
                                             (lc_df.StartDay == cal_df.LocalDay) & \
                                                (lc_df.StartHour == cal_df.LocalHour) & 
                                                 (lc_df.StartMinute == cal_df.LocalMinute), how="leftouter") 

new_data_start_df = new_data_start_df.drop('LocalYear', 'LocalMonth', 'LocalDay', 'LocalHour', 'LocalMinute', 'LocalDate', 'LocalTimeStamp', 'LocalTime', 'Holiday', 'Weekend')
new_data_start_df = new_data_start_df.drop('StartYear', 'StartMonth', 'StartDay', 'StartHour', 'StartMinute')

new_data_start_df = new_data_start_df.withColumnRenamed("MeterSampleIndex", "StartMeterSampleIndex") \
                            .withColumnRenamed("Interval", "StartInterval")

if debug:
    display(new_data_start_df)

# COMMAND ----------

# Add the end sample index.
new_data_df = new_data_start_df.join(cal_df, (new_data_start_df.EndYear == cal_df.LocalYear) & \
                                         (new_data_start_df.EndMonth == cal_df.LocalMonth) & \
                                             (new_data_start_df.EndDay == cal_df.LocalDay) & \
                                                (new_data_start_df.EndHour == cal_df.LocalHour) & 
                                                 (new_data_start_df.EndMinute == cal_df.LocalMinute), how="leftouter") 

new_data_df = new_data_df.drop('LocalYear', 'LocalMonth', 'LocalDay', 'LocalHour', 'LocalMinute', 'LocalDate', 'LocalTimeStamp', 'LocalTime', 'Holiday', 'Weekend')
new_data_df = new_data_df.drop('EndYear', 'EndMonth', 'EndDay', 'EndHour', 'EndMinute')

new_data_df = new_data_df.withColumnRenamed("MeterSampleIndex", "EndMeterSampleIndex") \
                            .withColumnRenamed("Interval", "EndInterval")

new_data_df = new_data_df.withColumn("TotalTime_Minutes", (col("EndMeterSampleIndex") - col("StartMeterSampleIndex")) * 5)

if debug:
    display(new_data_df)

# COMMAND ----------

# Check for null sample indexes.
null_check_df = new_data_df.filter(col("StartMeterSampleIndex").isNull() | col("EndMeterSampleIndex").isNull())

if null_check_df.count() > 0:
    print(null_check_df.count())
    display(null_check_df)
    raise Exception("CreateIndexedMeterData: Null MeterSampleIndex exists. Execution aborted.  Please investigate.")
else:
    print("CreateIndexedLoadControlPeriods: No null MeterSampleIndex found.")

# COMMAND ----------

# Add a unique ID to each row.  This will be used as the key for relationships with other datasets.
new_data_df = new_data_df.withColumn('LoadControlEventID', concat_ws("_", col('ResourceName'), col('StartDateTime').cast('string'), col('EndDateTime').cast('string')))

if debug:
    display(new_data_df)

# COMMAND ----------

new_data_df.write.format("delta").mode("overwrite").save(LOAD_CONTROL_INDEX_PATH)

# COMMAND ----------

# Clean up
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, LOAD_CONTROL_INDEX_PATH)
delta_table.vacuum()
