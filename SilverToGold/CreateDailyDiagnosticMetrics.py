# Databricks notebook source
# MAGIC %md
# MAGIC # Create Daily Diagnostic Metrics
# MAGIC Creates metrics useful for monitoring the ingestion pipelines.

# COMMAND ----------

# Imports and debug
from pyspark.sql.functions import col
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
index_path = MDM_INDEXED_PATH

if debug:
    print(uri)
    print(index_path)

# COMMAND ----------

# Read the indexed data.
index_df = spark.read.format("delta").load(index_path)

if debug:
    display(index_df)

# COMMAND ----------

from pyspark.sql.functions import year, month, day
index_df = index_df.withColumn("Year", year(col("StartDateTime"))) \
                    .withColumn("Month", month(col("StartDateTime"))) \
                        .withColumn("Day", day(col("StartDateTime")))

# COMMAND ----------

from pyspark.sql.functions import count

daily_df = index_df.groupBy("MeterNumber", "Year", "Month", "Day").agg(count("*").alias("count"))

# COMMAND ----------

daily_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(MDM_DIAGNOSTICS_PATH)

# COMMAND ----------

# Clean history
daily_table = DeltaTable.forPath(spark, MDM_DIAGNOSTICS_PATH)

daily_table.vacuum()
