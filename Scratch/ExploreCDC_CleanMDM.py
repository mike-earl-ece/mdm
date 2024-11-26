# Databricks notebook source
# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

set_spark_config()

# COMMAND ----------

uri = "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/"
mock_ingest_table = "default.bronze_mock_mdmingest"
mock_cleaned_table = "default.silverconformed_mock_mdmcleaned"


# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# Get last change from the cleaned data.
clean_changes_df = spark.read \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table(mock_cleaned_table)

display(clean_changes_df)

if clean_changes_df.count() != 0:
        last_change = clean_changes_df.select("_commit_timestamp").orderBy("_commit_timestamp", ascending=False).first()[0]
        print(last_change)


# COMMAND ----------

from pyspark.sql.utils import AnalysisException

found_changes = True # Will be set to False if no changes.

# Get the changes from the mock ingest table
if clean_changes_df.count() == 0:
        ingest_changes_df = spark.read \
                .option("readChangeFeed", "true") \
                .option("startingVersion", 0) \
                .table(mock_ingest_table)
else:
        try:
                ingest_changes_df = spark.read \
                        .option("readChangeFeed", "true") \
                        .option("startingTimestamp", last_change) \
                        .table(mock_ingest_table)
        except AnalysisException as e:
                if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in str(e):
                        print("No changes found after the last commit timestamp.")
                found_changes = False

display(ingest_changes_df)


# COMMAND ----------

# Drop the change tracking information from the ingest changes.
if found_changes:
    ingest_changes_df = ingest_changes_df.drop("_rescued_data", "_change_type", "_commit_version", "_commit_timestamp") 
    display(ingest_changes_df)

# COMMAND ----------

from delta.tables import DeltaTable

# Convert the DataFrame to a DeltaTable
if found_changes:
    if clean_changes_df.count() != 0:
        clean_table = DeltaTable.forName(spark, "silverconformed_mock_mdmcleaned")

        # Do an upsert of the changes.
        clean_table.alias('clean') \
            .merge(ingest_changes_df.alias('ingest'), 
            'clean.MeterNumber = ingest.MeterNumber AND clean.Channel = ingest.Channel AND clean.FlowDirection = ingest.FlowDirection AND clean.StartDateTime = ingest.StartDateTime AND clean.EndDateTime = ingest.EndDateTime') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    # Else just insert the new data
    else:  
        ingest_changes_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "True") \
                .save(uri + "SilverConformed_Mock/MDM/Cleaned")


# COMMAND ----------

clean_df = spark.read.table("silverconformed_mock_mdmcleaned")

print(clean_df.count())
