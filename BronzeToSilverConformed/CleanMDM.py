# Databricks notebook source
# MAGIC %md
# MAGIC # Clean MDM
# MAGIC Transforms ingested data from Bronze into deduplicated and updated data in SilverConformed. 

# COMMAND ----------

# Imports and debug
from pyspark.sql.functions import lit, to_timestamp, max
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
# MAGIC ## Data cleaning
# MAGIC The following section updates the MDM data in the SilverConformed tier based new data that has come into the Bronze ingestion table since the last update to SilverConformed.  Specifically:
# MAGIC - Use the change history of the SilverConformed table to get the last change date.
# MAGIC - Use the change history of the Bronze table to get all changes since the last SilverConformed change date.
# MAGIC - Do an upsert of the new data into the SilverConformed table.
# MAGIC   - If there is a matching meter, channel, direction, and start/end timestamps, update the record.
# MAGIC   - Otherwise insert the record.
# MAGIC
# MAGIC The only 'cleaning' of the MDM data is updating records with new information that might be in the latest ingested data.

# COMMAND ----------

if debug:
    try:
        pre_update_df = spark.read.table(output_table_name)
        print("Before update count: " + str(pre_update_df.count()))
    except AnalysisException as e:
        print(str(e))


# COMMAND ----------


# Get last change from the cleaned data.
clean_changes_df = spark.read \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table(output_table_name)

if debug:
    display(clean_changes_df)

if clean_changes_df.count() != 0:
        last_change = clean_changes_df.select("_commit_timestamp").orderBy("_commit_timestamp", ascending=False).first()[0]
        print(last_change)

# COMMAND ----------


# Find the changes in the Bronze ingest table.  
found_changes = True # Will be set to False if no changes.

# If the clean table is empty, get all the changes.
if clean_changes_df.count() == 0:
        ingest_changes_df = spark.read \
                .option("readChangeFeed", "true") \
                .option("startingVersion", 0) \
                .table(input_table_name)
# If the clean table has rows, get the changes since the last change.  If there are no changes, an exception will be 
# thrown, caught, and found_changes will be set to False.
else:
        try:
                ingest_changes_df = spark.read \
                        .option("readChangeFeed", "true") \
                        .option("startingTimestamp", last_change) \
                        .table(input_table_name)
        except AnalysisException as e:
                if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in str(e):
                        print("No changes found after the last commit timestamp.")
                found_changes = False

if debug and found_changes:
    display(ingest_changes_df)


# COMMAND ----------

# Drop the change tracking information from the ingest changes.
if found_changes:
    ingest_changes_df = ingest_changes_df.drop("_rescued_data", "_change_type", "_commit_version", "_commit_timestamp") 
    if debug:
        display(ingest_changes_df)

# COMMAND ----------

# Upsert the changes to the clean table
if found_changes:
    if clean_changes_df.count() != 0:
        # Convert the DataFrame to a DeltaTable
        clean_table = DeltaTable.forName(spark, output_table_name)

        # Do an upsert of the changes.
        clean_table.alias('clean') \
            .merge(ingest_changes_df.alias('ingest'), 
            'clean.MeterNumber = ingest.MeterNumber AND clean.Channel = ingest.Channel AND clean.FlowDirection = ingest.FlowDirection AND clean.StartDateTime = ingest.StartDateTime AND clean.EndDateTime = ingest.EndDateTime') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    # Else just insert the new data (clean table is empty)
    else:  
        insert_changes_df = insert_changes_df.dropDuplicates()
        ingest_changes_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "True") \
                .save(MDM_CLEANED_PATH)

# COMMAND ----------



# COMMAND ----------

if debug:
    clean_out_df = spark.read.table(output_table_name)
    print(clean_out_df.count())
    display(clean_out_df)

# COMMAND ----------

# Look for duplicate rows.  This likely indicates a problem with the upsert.
if debug:
    display(clean_out_df.groupBy(clean_out_df.columns).count().filter("count > 1").orderBy("count", ascending=False))
