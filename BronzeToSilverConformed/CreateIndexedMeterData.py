# Databricks notebook source
# MAGIC %md
# MAGIC # Create Indexed Meter Data
# MAGIC Transforms the cleaned MDM data by adding index and other information from the Indexed Calendar.
# MAGIC
# MAGIC This is done incrementally by finding all chagnes in the cleaned file since the last update to the indexed file.

# COMMAND ----------

# Imports and debug
from pyspark.sql.functions import col, year, month, day, hour, minute
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

debug = 1

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

# Initialize key variables. The constants are defined in ConfigUtilities.
uri = CONTAINER_URI_PATH
input_table_name = MDM_CLEANED_TABLE
output_table_name = MDM_INDEXED_TABLE

if debug:
    print(uri)
    print(input_table_name)
    print(output_table_name)

# COMMAND ----------

# Get last change from the indexed data history.
from delta.tables import *

index_table = DeltaTable.forPath(spark, MDM_INDEXED_PATH)
history_df = index_table.history()

merge_history_df = history_df.filter((col('operation')=="MERGE") | (col('operation')=="WRITE"))

if (merge_history_df.count() > 0):
    last_change = merge_history_df.select("timestamp").orderBy("timestamp", ascending=False).first()[0]
    index_has_data = True
else:
    index_has_data = False

if debug:
    display(history_df)
    print("Index has data:" + str(index_has_data))
    if index_has_data:
        print(last_change)


# COMMAND ----------

# Get output table count.
if debug and index_has_data:
    pre_update_df = spark.read.table(output_table_name)
    print("Before update count: " + str(pre_update_df.count()))


# COMMAND ----------

# Get changes from the cleaned file since the last update to the indexed meter data.
found_clean_changes = True # Will be set to False if no changes.

# If the clean table is empty, get all the data.
if index_has_data == False:
        clean_changes_all_df = spark.read \
                .table(input_table_name)
# If the indexed table has rows, get the changes since the last change.  If there are no changes, an exception will be 
# thrown, caught, and found_changes will be set to False.
else:
        try:
            clean_changes_all_df = spark.read \
                    .option("readChangeFeed", "true") \
                    .option("startingTimestamp", last_change) \
                    .table(input_table_name)
        except AnalysisException as e:
            if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in str(e):
                    print("No changes found after the last commit timestamp.")
            found_clean_changes = False

if debug: 
        if found_clean_changes:
            display(clean_changes_all_df)
        else:
            print("No clean changes found.")

# No need to continue if there are no clean changes found.
if found_clean_changes == False:
        dbutils.notebook.exit("No clean changes found.") 


# COMMAND ----------

# When an update happens on the input table, there are two rows added to the change list - one representing the new row and one representing the old row.  
# We need to remove the old row from the change set by filtering out _change_types with the values update_preimage.
if index_has_data:
    clean_changes_filter_df = clean_changes_all_df.filter(col('_change_type') != "update_preimage")

    # Deletes are unlikely, but can happen if some maintenance was done on the input file.  We need to remove these from the change set.
    clean_changes_filter_df = clean_changes_filter_df.filter(col('_change_type') != "delete")

    if debug:
        print(clean_changes_filter_df.count())

# COMMAND ----------

# Clean up changes to import
if index_has_data:
    clean_changes_filter_df = clean_changes_filter_df.drop("_rescued_data", "_change_type", "_commit_version", "_commit_timestamp") 
else:
    clean_changes_filter_df = clean_changes_all_df

if debug:
    display(clean_changes_filter_df)

# COMMAND ----------

# Check for duplicates on the input table.
duplicates_df = clean_changes_filter_df.groupBy(clean_changes_filter_df.columns).count().filter("count > 1")

if duplicates_df.count() > 0:
    clean_changes_filter_df = clean_changes_filter_df.dropDuplicates()
    if debug:
        print(duplicates_df.count())
        display(duplicates_df)
        display(clean_changes_filter_df)
else:
    print("No full duplicates found on the input data.")

# COMMAND ----------

# Look for duplicates using the upsert columns.  Duplicates here will cause the upsert to fail.
duplicates_df = clean_changes_filter_df.groupBy("MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime").count().filter("count > 1")

if debug:
  display(duplicates_df.orderBy("count", ascending=False))

if duplicates_df.count() > 0:
  print("Duplicates still exist: " + str(duplicates_df.count()))
  # Join with the original dataset and display. 
  clean_changes_dup_df = duplicates_df.join(clean_changes_all_df, ["MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime"], "left")
  print(clean_changes_dup_df.count())
  display(clean_changes_dup_df)

  raise Exception("CreateIndexedMeterData: Duplicates exist in clean data before insert to index. Aborting script. Please review the data and correct the issue.")
else:
  print("No duplicates found on the input data.")

# COMMAND ----------

# Read the indexed calendar
cal_df =  spark.read.parquet(INDEXED_CALENDAR_PATH)

if debug:
    display(cal_df)

# COMMAND ----------

# Join datasets by time.  We want the meter sample index for both the start and end times.

# Add time columns to the cleaned data for the joins.
clean_changes_filter_df = clean_changes_filter_df.withColumn("StartYear", year(col("StartDateTime"))) \
                                    .withColumn("StartMonth", month(col("StartDateTime"))) \
                                    .withColumn("StartDay", day(col("StartDateTime"))) \
                                    .withColumn("StartHour", hour(col("StartDateTime"))) \
                                    .withColumn("StartMinute", minute(col("StartDateTime")))
clean_changes_filter_df = clean_changes_filter_df.withColumn("EndYear", year(col("EndDateTime"))) \
                                    .withColumn("EndMonth", month(col("EndDateTime"))) \
                                    .withColumn("EndDay", day(col("EndDateTime"))) \
                                    .withColumn("EndHour", hour(col("EndDateTime"))) \
                                    .withColumn("EndMinute", minute(col("EndDateTime")))

if debug:
    display(clean_changes_filter_df)

# COMMAND ----------

# Add the start sample index.
new_data_start_df = clean_changes_filter_df.join(cal_df, (clean_changes_filter_df.StartYear == cal_df.Year) & \
                                         (clean_changes_filter_df.StartMonth == cal_df.Month) & \
                                             (clean_changes_filter_df.StartDay == cal_df.Day) & \
                                                (clean_changes_filter_df.StartHour == cal_df.Hour) & 
                                                 (clean_changes_filter_df.StartMinute == cal_df.Minute), how="leftouter") 

new_data_start_df = new_data_start_df.drop('Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'TimeStamp', 'Time')
new_data_start_df = new_data_start_df.drop('StartYear', 'StartMonth', 'StartDay', 'StartHour', 'StartMinute')

new_data_start_df = new_data_start_df.withColumnRenamed("MeterSampleIndex", "StartMeterSampleIndex") \
                            .withColumnRenamed("Interval", "StartInterval")

if debug:
    display(new_data_start_df)

# COMMAND ----------

# Add the end sample index.
new_data_df = new_data_start_df.join(cal_df, (new_data_start_df.EndYear == cal_df.Year) & \
                                         (new_data_start_df.EndMonth == cal_df.Month) & \
                                             (new_data_start_df.EndDay == cal_df.Day) & \
                                                (new_data_start_df.EndHour == cal_df.Hour) & 
                                                 (new_data_start_df.EndMinute == cal_df.Minute), how="leftouter") 

new_data_df = new_data_df.drop('TimeStamp', 'Time', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Date', 'TimeStamp', 'Time')
new_data_df = new_data_df.drop('EndYear', 'EndMonth', 'EndDay', 'EndHour', 'EndMinute')

new_data_df = new_data_df.withColumnRenamed("MeterSampleIndex", "EndMeterSampleIndex") \
                            .withColumnRenamed("Interval", "EndInterval")

new_data_df = new_data_df.withColumn("SampleRate", (col("EndMeterSampleIndex") - col("StartMeterSampleIndex")) * 5)

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
    print("CreateIndexedMeterData: No null MeterSampleIndex found.")


# COMMAND ----------

# Upsert the changes to the indexed table
if index_has_data:
    # Convert the DataFrame to a DeltaTable
    indexed_table = DeltaTable.forName(spark, output_table_name)

    # Do an upsert of the changes.
    indexed_table.alias('index') \
        .merge(new_data_df.alias('clean'), 
        'clean.UnitOfMeasure = index.UnitOfMeasure AND clean.MeterNumber = index.MeterNumber AND clean.Channel = index.Channel AND clean.FlowDirection = index.FlowDirection AND clean.StartDateTime = index.StartDateTime AND clean.EndDateTime = index.EndDateTime') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
# Else just insert the new data (indexed table is empty)
else:  
    new_data_df = new_data_df.dropDuplicates()
    new_data_df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "True") \
            .save(MDM_INDEXED_PATH)

# COMMAND ----------

if debug:
    indexed_out_df = spark.read.table(output_table_name)
    print(indexed_out_df.count())
    display(indexed_out_df)

# COMMAND ----------

# Look for duplicate rows.  This likely indicates a problem with the upsert.

# This an expensive operation, commenting out for now.
#if debug:
#    display(indexed_out_df.groupBy(indexed_out_df.columns).count().filter("count > 1").orderBy("count", ascending=False))

# COMMAND ----------

# Clean up history
index_table.vacuum()
