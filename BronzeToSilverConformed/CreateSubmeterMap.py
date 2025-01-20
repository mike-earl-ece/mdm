# Databricks notebook source
# MAGIC %md
# MAGIC # Create Submeter Map
# MAGIC Using the meter info exported from iVUE, create a table that maps submeters to main meters.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

# Set up the environment using a function in ConfigUtilties.
set_spark_config()

# COMMAND ----------

uri = CONTAINER_URI_PATH

input_data_path = f"{uri}/Bronze/iVUE/DIM_MeterInfo/DIM_MeterInfo.csv"

debug = 1

# COMMAND ----------

meter_info_df =  spark.read.option("header", True).csv(input_data_path)

print(meter_info_df.count())
display(meter_info_df)
display(meter_info_df.select('BI_MTR_NBR').distinct().count())

# COMMAND ----------

display(meter_info_df.filter(col('BI_REGISTER_SET_NBR')!=1))

# COMMAND ----------

display(meter_info_df.filter(col('BI_MTR_NBR')==33535968))

# COMMAND ----------

# Subset the columns for the meter info and then get the sub-meters.
meter_info_df = meter_info_df.select(['BI_ACCT', 'BI_MTR_NBR', 'BI_MTR_POS_NBR', 'BI_MTR_CONFIG'])

sub_meter_df = meter_info_df.filter(meter_info_df.BI_MTR_CONFIG=='S')


print(sub_meter_df.count())
display(sub_meter_df)
display(sub_meter_df.groupBy("BI_MTR_POS_NBR").count().orderBy("BI_MTR_POS_NBR"))

# COMMAND ----------

# Join the sub meters to all other meters on the account.

# First rename the columns for all meters to avoid collisions.  Note - setting the meter number column name so it will be right after the filtering.
meter_info_for_join_df = meter_info_df.withColumnRenamed('BI_MTR_NBR', 'MainMeter')\
                            .withColumnRenamed('BI_MTR_POS_NBR', 'OtherMeterPosNbr')\
                            .withColumnRenamed('BI_MTR_CONFIG', 'OtherMeterConfig')

sub_meter_plus_df = sub_meter_df.join(meter_info_for_join_df, on='BI_ACCT', how='leftouter')

# Make sure all the submeters have other meters associated with the account.
print("Dataframe count aFter joining with other meters (count should be higher): ", sub_meter_plus_df.count())
print("Null other meters from dimensions file (should be 0):  ", sub_meter_plus_df.filter(sub_meter_plus_df.MainMeter.isNull()).count())  

# COMMAND ----------

# Filter down to the rows where the other meter is the main meter.
sub_meter_main_df = sub_meter_plus_df.filter(sub_meter_plus_df.OtherMeterPosNbr==1)

print("Dataframe count aFter filtering to the main meter (count should be same as original submeter count): ", sub_meter_main_df.count())

display(sub_meter_main_df)


# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_meters_df = sub_meter_main_df.groupBy('BI_MTR_NBR').agg(count('*').alias('count')).filter(col('count') > 1)

display(duplicate_meters_df)

# COMMAND ----------

display(sub_meter_main_df.filter(col('BI_MTR_NBR')==33535968))

# COMMAND ----------

# Find submeters that don't have a main meter.
sub_without_main_df = sub_meter_df.join(sub_meter_main_df, on='BI_MTR_NBR', how='leftanti')

display(sub_without_main_df)

# COMMAND ----------

# Drop columns that are not interesting and save.
sub_meter_main_final_df = sub_meter_main_df.drop('OtherMeterPosNbr', 'OtherMeterConfig')

display(sub_meter_main_final_df)
print(sub_meter_main_final_df.count())


# COMMAND ----------

# Output to CSV
csv_out_file = f"{uri}/SilverConformed/iVUE/DIM_SubMeterMap/CSV"
sub_meter_main_final_df.coalesce(1).write.option("header",True).mode('overwrite').csv(csv_out_file)


# COMMAND ----------

# Output to Parquet
parquet_out_file = f"{uri}/SilverConformed/iVUE/DIM_SubMeterMap/Parquet"
sub_meter_main_final_df.coalesce(1).write.option("header",True).mode('overwrite').parquet(parquet_out_file)

