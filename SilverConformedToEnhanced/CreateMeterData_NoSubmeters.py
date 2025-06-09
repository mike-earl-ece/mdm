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
from delta.tables import DeltaTable

debug=1

# COMMAND ----------

# To incrementally update the output table, we need to find the last index that has been processed.
try:
    mdm_nosub_df = spark.read.format('delta').load(MDM_NO_SUBMETERS)
    last_processed_index = mdm_nosub_df.select(max('EndMeterSampleIndex')).collect()[0][0]
except:   # Table is empty
    print("Exception - table is empty.")
    last_processed_index= 1

if debug:
    print(last_processed_index)

# COMMAND ----------

# Meter data
data_path = MDM_INDEXED_PATH

mdm_df =  spark.read.format("delta").load(data_path).filter(col('EndMeterSampleIndex') > last_processed_index)

if mdm_df.count() == 0:
    dbutils.notebook.exit("No new data found.") 

if debug:
    display(mdm_df)


# COMMAND ----------

# Submeter info.
data_path = DIM_SUBMETER_MAP_PATH

submeter_info_df =  spark.read.format("delta").load(data_path)

if debug:
    print(submeter_info_df.count())
    display(submeter_info_df)


# COMMAND ----------

# Do a left anti join to remove the submeters from the main data set.
main_df = mdm_df.join(submeter_info_df, (mdm_df.MeterNumber == submeter_info_df.BI_MTR_NBR), how='leftanti')

if debug:
    display(main_df)
    # Look for a specific sub-meter to confirm it's not there.
    display(main_df.filter(col('MeterNumber')=='83212702'))



# COMMAND ----------

# Save the data.
main_df.write.mode('append').format('delta').option("mergeSchema", "true").save(MDM_NO_SUBMETERS)

# COMMAND ----------

# Clean up the delta history.
spark.sql(f"VACUUM '{MDM_NO_SUBMETERS}'")
