# Databricks notebook source
# MAGIC %md
# MAGIC # Create Meter Data - No Submeters
# MAGIC
# MAGIC Process the meter data to remove the sub-meters that get rolled up to the main meters.  This will be useful for some downstream processing, particularly when accurate total usage numbers are desired.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col, max
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

debug=1

# COMMAND ----------

uri = CONTAINER_URI_PATH
upstream_table_name = MDM_INDEXED_TABLE
downstream_table_name = MDM_NO_SUBMETERS_TABLE
downstream_table_path = MDM_NO_SUBMETERS_PATH

# COMMAND ----------

# Get last change from the downstream table history (no submeters data).
downstream_table = DeltaTable.forPath(spark, downstream_table_path)
history_df = downstream_table.history()

merge_history_df = history_df.filter((col('operation')=="MERGE") | (col('operation')=="WRITE"))

if (merge_history_df.count() > 0):
    last_change = merge_history_df.select("timestamp").orderBy("timestamp", ascending=False).first()[0]
    downstream_has_data = True
else:
    downstream_has_data = False

if debug:
    display(history_df)
    print("Downstream has data:" + str(downstream_has_data))
    if downstream_has_data:
        print(last_change)

# COMMAND ----------

# Get changes from the upstream table since the last update to the downstream table.

# If the downstream  table is empty, get all the changes.
if downstream_has_data == False:
        upstream_changes_all_df = spark.read \
                .table(upstream_table_name)
# If the downstream table has data, get the upstream changes.  If there are no changes, an exception will be 
# thrown, caught, and found_changes will be set to False.
else:
        try:
            upstream_changes_all_df = spark.read \
                    .option("readChangeFeed", "true") \
                    .option("startingTimestamp", last_change) \
                    .table(upstream_table_name)
        except AnalysisException as e:
            if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in str(e):
                print("No changes found after the last commit timestamp.")
                # No need to continue if there are no clean changes found.
                dbutils.notebook.exit("No upstream changes found.") 

if debug: 
        display(upstream_changes_all_df)
        print("Upstream changes count: " + str(upstream_changes_all_df.count()))

# COMMAND ----------

# When an update happens on the input table, there are two rows added to the change list - one representing the new row and one representing the old row.  
# We need to remove the old row from the change set by filtering out _change_types with the values update_preimage.
if downstream_has_data:
    upstream_changes_filter_df = upstream_changes_all_df.filter(col('_change_type') != "update_preimage")

    # Deletes are unlikely, but can happen if some maintenance was done on the input file.  We need to remove these from the change set.
    upstream_changes_filter_df = upstream_changes_filter_df.filter(col('_change_type') != "delete")

    if debug:
        print(upstream_changes_filter_df.count())

# COMMAND ----------

# Clean up changes to import
if downstream_has_data:
    upstream_changes_filter_df = upstream_changes_filter_df.drop("_rescued_data", "_change_type", "_commit_version", "_commit_timestamp") 
else:
    upstream_changes_filter_df = upstream_changes_all_df

if debug:
    display(upstream_changes_filter_df)

# COMMAND ----------

# Submeter info.
data_path = DIM_SUBMETER_MAP_PATH

submeter_info_df =  spark.read.format("delta").load(data_path)

if debug:
    print(submeter_info_df.count())
    display(submeter_info_df)


# COMMAND ----------

# Do a left anti join to remove the submeters from the main data set.
new_upstream_data_df = upstream_changes_filter_df.join(submeter_info_df, (upstream_changes_filter_df.MeterNumber == submeter_info_df.BI_MTR_NBR), how='leftanti')

if debug:
    display(new_upstream_data_df)
    # Look for a specific sub-meter to confirm it's not there.
    display(new_upstream_data_df.filter(col('MeterNumber')=='83212702'))



# COMMAND ----------

# Upsert the changes to the downstream table
if downstream_has_data:
    # Convert the DataFrame to a DeltaTable
    downstream_table = DeltaTable.forName(spark, downstream_table_name)

    # Do an upsert of the changes.
    downstream_table.alias('down') \
        .merge(new_upstream_data_df.alias('up'), 
        'up.UnitOfMeasure = down.UnitOfMeasure AND up.MeterNumber = down.MeterNumber AND up.Channel = down.Channel AND up.FlowDirection = down.FlowDirection AND up.StartMeterSampleIndex = down.StartMeterSampleIndex AND up.EndMeterSampleIndex = down.EndMeterSampleIndex') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
# Else just insert the new data (downstream table is empty)
else:  
    new_upstream_data_df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "True") \
            .save(downstream_table_path)

# COMMAND ----------

# Clean up the delta history.
spark.sql(f"VACUUM '{MDM_NO_SUBMETERS_PATH}'")
