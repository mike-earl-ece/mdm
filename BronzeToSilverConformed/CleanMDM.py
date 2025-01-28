# Databricks notebook source
# MAGIC %md
# MAGIC # Clean MDM
# MAGIC Transforms ingested data from Bronze into deduplicated and updated data in SilverConformed. 

# COMMAND ----------

# Imports and debug
from pyspark.sql.functions import lit, to_timestamp, max, col, when
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

debug = 1

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

# Initialize key variables (constants defined in ConfigUtilities)
uri = CONTAINER_URI_PATH
input_table_name = MDM_INGEST_TABLE
output_table_name = MDM_CLEANED_TABLE

if debug:
    print(uri)
    print(input_table_name)
    print(output_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental data update
# MAGIC The following section updates the MDM data in the SilverConformed tier based new data that has come into the Bronze ingestion table since the last update to SilverConformed.  Specifically:
# MAGIC - Use the change history of the SilverConformed table to get the last change date.
# MAGIC - Use the change history of the Bronze table to get all changes since the last SilverConformed change date.
# MAGIC - Do an upsert of the new data into the SilverConformed table.
# MAGIC   - If there is a matching meter, channel, direction, and start/end timestamps, update the record.
# MAGIC   - Otherwise insert the record.
# MAGIC
# MAGIC The only 'cleaning' of the MDM data is updating records with new information that might be in the latest ingested data.

# COMMAND ----------

# Get last change from the cleaned data history.
from delta.tables import *

clean_table = DeltaTable.forPath(spark, MDM_CLEANED_PATH)
history_df = clean_table.history()

merge_history_df = history_df.filter(col('operation')=="MERGE")

if (merge_history_df.count() > 0):
    last_change = merge_history_df.select("timestamp").orderBy("timestamp", ascending=False).first()[0]
    clean_has_data = True
else:
    clean_has_data = False

if debug:
    display(history_df)
    print(clean_has_data)
    if clean_has_data:
        print(last_change)


# COMMAND ----------

if debug and clean_has_data:
    pre_update_df = spark.read.table(output_table_name)
    print("Before update count: " + str(pre_update_df.count()))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the new data from the bronze ingest table and do some cleaning.

# COMMAND ----------


# Find the changes in the Bronze ingest table.  
found_ingest_changes = True # Will be set to False if no changes.

# If the clean table has no data, get all the changes.
if clean_has_data == False:
        ingest_changes_all_df = spark.read \
                .option("readChangeFeed", "true") \
                .option("startingVersion", 0) \
                .table(input_table_name)
# If the clean table has data, get the ingested changes since the last change.  If there are no changes to ingestion data, an exception will be 
# thrown, caught, and found_changes will be set to False.
else:
        try:
                ingest_changes_all_df = spark.read \
                        .option("readChangeFeed", "true") \
                        .option("startingTimestamp", last_change) \
                        .table(input_table_name)
        except AnalysisException as e:
                if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in str(e):
                        print("No changes found after the last commit timestamp.")
                found_ingest_changes = False

if debug:
        if found_ingest_changes:
                display(ingest_changes_all_df)
        else:
                print("No ingest changes found.")

# No need to continue if there are no ingest changes found.
if found_ingest_changes == False:
        dbutils.notebook.exit("No ingest changes found.") 

# COMMAND ----------

# When an update happens on the input table, there are two rows added to the change list - one representing the new row and one representing the old row.  
# We need to remove the old row from the change set by filtering out _change_types with the values update_preimage.
ingest_changes_all_df = ingest_changes_all_df.filter(col('_change_type') != "update_preimage")

# Deletes are unlikely, but can happen if some maintenance was done on the input file.  We need to remove these from the change set.
ingest_changes_all_df = ingest_changes_all_df.filter(col('_change_type') != "delete")

if debug:
  print(ingest_changes_all_df.count())

# COMMAND ----------

# MDM extract appears to have some duplication issues. This can also be due to backfilling 
# or overlapping ingestions with changed values.  Deal with that here.

# First, there are some duplicates in the data, so drop these after removing the metadata columns.
ingest_changes_df = ingest_changes_all_df.drop("_rescued_data", "_change_type", "_commit_version", "_commit_timestamp") 
ingest_changes_df = ingest_changes_df.dropDuplicates()

if debug:
  print(ingest_changes_df.count())

# Second, there are some rows that have duplicate metadata columns (see below), but with an AMIValue equal to 0 and a null VEEValue.  These rows need to be removed.
# Find duplicates.
subset_df = ingest_changes_df.select("MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime")
duplicates_df = subset_df.groupBy("MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime").count().filter("count > 1")
if debug:
  print(duplicates_df.count())

if duplicates_df.count() > 0:
  # Join back to main dataset; the count column will track duplicates.
  ingest_changes_dup_df = ingest_changes_df.join(duplicates_df, ["MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime"], "left")

  # Deal with null count values as they seem to have strange behavior in logic statements.
  ingest_changes_dup_df = ingest_changes_dup_df.withColumn("DupCount", when(col('count').isNull(), 1).otherwise(col('count')))

  if debug:
    print(ingest_changes_dup_df.count())

  # Handle the case where the VEEValue is null in the duplicate set.  
  ingest_valid_df = ingest_changes_dup_df.filter(~( (col('VEEValue').isNull()) & (col('DupCount') == 2) ) )

  # Now handle the case where the AMIValue is 0 and the VEEValue is 0 in the duplicate set.
  ingest_valid_df = ingest_valid_df.filter(~( (col('VEEValue') == 0) & (col('AMIValue') == 0) & (col('DupCount') == 2) ) )

  # Take this one step further and remove cases where the AMIValue is 0 in the duplicate set.
  ingest_valid_df = ingest_valid_df.filter(~( (col('AMIValue') == 0) & (col('DupCount') == 2) ) )

  ingest_valid_df = ingest_valid_df.drop('count', 'DupCount')

  if debug:
    print(ingest_valid_df.count())
    display(ingest_valid_df)  
else:
  ingest_valid_df = ingest_changes_df

# COMMAND ----------

 # Check for duplicates at this point,  If any exist, display some debug information and abort.
duplicates_df = ingest_valid_df.groupBy("MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime")\
                  .count().filter("count > 1")
  
if duplicates_df.count() > 0:
    print("Duplicates still exist: " + str(duplicates_df.count()))
    # Join with the original dataset and display. 
    ingest_changes_dup2_df = duplicates_df.join(ingest_changes_all_df, ["MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime"], "left")
    print(ingest_changes_dup2_df.count())
    display(ingest_changes_dup2_df)

    raise Exception("CleanMDM: Duplicates exist in data after cleaning. Aborting script. Please review the data and correct the issue.")
else:
  print("No duplicates found.")

# COMMAND ----------

max_time_df = ingest_changes_df.select("EndDateTime").distinct().orderBy("EndDateTime", ascending=True)
display(max_time_df)

# COMMAND ----------

# Upsert the changes to the clean table
if found_ingest_changes:
    if clean_has_data:
        # Convert the DataFrame to a DeltaTable
        clean_table = DeltaTable.forName(spark, output_table_name)

        # Do an upsert of the changes.
        clean_table.alias('clean') \
            .merge(ingest_changes_df.alias('ingest'), 
            'clean.UnitOfMeasure = ingest.UnitOfMeasure AND clean.MeterNumber = ingest.MeterNumber AND clean.Channel = ingest.Channel AND clean.FlowDirection = ingest.FlowDirection AND clean.StartDateTime = ingest.StartDateTime AND clean.EndDateTime = ingest.EndDateTime') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    # Else just insert the new data (clean table is empty)
    else:  
        ingest_changes_df = ingest_changes_df.dropDuplicates()
        ingest_changes_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "True") \
                .save(MDM_CLEANED_PATH)

# COMMAND ----------

clean_out_df = spark.read.table(output_table_name)
print(clean_out_df.count())
display(clean_out_df)

# COMMAND ----------

 # Check for duplicates at this point,  This likely indicates a problem with the upsert.  Abort if there is an issue.
duplicates_out_df = clean_out_df.groupBy("MeterNumber", "UnitOfMeasure", "FlowDirection", "Channel", "StartDateTime", "EndDateTime")\
                  .count().filter("count > 1")

dup_count = duplicates_out_df.count()

if dup_count > 0: 
  print(dup_count)
  display(duplicates_out_df)

  raise Exception("Duplicates found in the clean table.  Please investigate.")

else:
  print("No duplicates found after the upsert.")

# COMMAND ----------

# Clean up history
clean_table.vacuum()
