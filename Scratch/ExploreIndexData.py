# Databricks notebook source
# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

# Initialize key variables. The constants are defined in ConfigUtilities.
uri = CONTAINER_URI_PATH
index_path = MDM_INDEXED_PATH


# COMMAND ----------

# Read the indexed data.
index_df = spark.read.format("delta").load(index_path)

display(index_df)

# COMMAND ----------

from pyspark.sql.functions import col

distinct_meter_numbers_df = index_df.select(col("MeterNumber")).distinct()
display(distinct_meter_numbers_df)
print(distinct_meter_numbers_df.count())

# COMMAND ----------

from pyspark.sql.functions import col

display(index_df.filter(col('MeterNumber')==20422065))
