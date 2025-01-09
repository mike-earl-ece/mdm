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

# Find the last update to the indexed meter data.
index_has_data = True

try:
        indexed_changes_df = spark.read \
                .option("readChangeFeed", "true") \
                .option("startingVersion", 0) \
                .table(output_table_name)
        if debug:
                display(indexed_changes_df)

        if indexed_changes_df.count() != 0:
                last_change = indexed_changes_df.select("_commit_timestamp").orderBy("_commit_timestamp", ascending=False).first()[0]
                print(last_change)
                index_has_data = True
        else:
                index_has_data = False
except AnalysisException as e:
        print(str(e))
        index_has_data = False

if debug:
        print("Index has data: " + str(index_has_data))

# COMMAND ----------

# Get output table count.
if debug and index_has_data:
    pre_update_df = spark.read.table(output_table_name)
    print("Before update count: " + str(pre_update_df.count()))


# COMMAND ----------

# Get changes from the cleaned file since the last update to the indexed meter data.
found_clean_changes = True # Will be set to False if no changes.

# If the clean table is empty, get all the changes.
if index_has_data == False:
        clean_changes_all_df = spark.read \
                .option("readChangeFeed", "true") \
                .option("startingVersion", 0) \
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
clean_changes_filter_df = clean_changes_all_df.filter(col('_change_type') != "update_preimage")

# Deletes are unlikely, but can happen if some maintenance was done on the input file.  We need to remove these from the change set.
clean_changes_filter_df = clean_changes_filter_df.filter(col('_change_type') != "delete")

if debug:
    print(clean_changes_filter_df.count())

# COMMAND ----------

# Clean up changes to import
if found_clean_changes:
    clean_changes_filter_df = clean_changes_filter_df.drop("_rescued_data", "_change_type", "_commit_version", "_commit_timestamp") 
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
  raise Exception("CreateIndexedMeterDAta: Duplicates exist in clean data before insert to index. Aborting script. Please review the data and correct the issue.")

# COMMAND ----------

# Read the indexed calendar
cal_df =  spark.read.parquet(INDEXED_CALENDAR_PATH)

if debug:
    display(cal_df)

# COMMAND ----------

# Join datasets by time.  Use the ending time for the meter data.

# Add time columns to the cleaned data for the join.
clean_changes_filter_df = clean_changes_filter_df.withColumn("Year", year(col("EndDateTime"))) \
                                    .withColumn("Month", month(col("EndDateTime"))) \
                                    .withColumn("Day", day(col("EndDateTime"))) \
                                    .withColumn("Hour", hour(col("EndDateTime"))) \
                                    .withColumn("Minute", minute(col("EndDateTime")))

if debug:
    display(clean_changes_filter_df)

# COMMAND ----------

new_data_df = clean_changes_df.join(cal_df, on=["Year","Month","Day","Hour", "Minute"], how="leftouter")

if debug:
    display(new_data_df)

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
indexed_table.vacuum()
