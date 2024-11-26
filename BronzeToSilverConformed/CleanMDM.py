# Databricks notebook source
# MAGIC %md
# MAGIC # Clean MDM
# MAGIC Cleans MDM data ingested into the Bronze tier. 

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Imports and debug
from pyspark.sql.functions import lit, to_timestamp, max

debug = 1

# COMMAND ----------

# Configure Spark
set_spark_config()

# COMMAND ----------

# Initialize key variables (constants defined in ConfigUtilites)
uri = CONTAINER_URI_PATH
checkpoint_path = MDM_CLEANED_PATH + "Checkpoint"
input_table_name = MDM_INGEST_TABLE
output_table_name = MDM_CLEANED_TABLE

if debug:
    print(uri)
    print(checkpoint_path)
    print(input_table_name)
    print(output_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data cleaning
# MAGIC The following section takes the following actions to prep the data for further processing:
# MAGIC - Remove the header row that got ingested from each file.
# MAGIC - Removes the _rescued_data column.  This column has information on the ingestion source for each data point.
# MAGIC - Drops duplicate rows to handle the situation when the same data points are ingested multiple times.
# MAGIC
# MAGIC This is done incrementally for new data that was ingested. This is based on the last update date of the cleaned data.
# MAGIC The resulting data set is upserted to the Silver folder in delta format.

# COMMAND ----------

cleaned_df = spark.read.table(output_table_name)

if cleaned_df.count() != 0:
    last_update_df = cleaned_df.agg(max('EndDateTime').alias('LastSampleTime'))
    last_update_timestamp = last_update_df.first()[0]
else:
    last_update_timestamp = to_timestamp(lit("2023-01-01"), "yyyy-MM-dd")   # Initialized only table

print(last_update_timestamp)

# COMMAND ----------



# COMMAND ----------

ingest_df = spark.read.table(input_table_name)

if debug:
    display(ingest_df)

new_ingest_df = ingest_df.filter(ingest_df.StartDateTime > last_update_timestamp)

if debug:
    display(new_ingest_df)

# COMMAND ----------

df2 = spark.read.table(input_table_name)
