# Databricks notebook source
# MAGIC %md
# MAGIC # Create GRE Monthly Load
# MAGIC Using the spreadsheets that are provided by GRE on a monthly basis, this script extracts the coincidental peak hour and the monthly loads by substation.
# MAGIC
# MAGIC Note that the GRE spreadsheets are not well structured and information location is hard coded in this script, making the lookups fragile.

# COMMAND ----------

# MAGIC %run ../Utilities/ConfigUtilities

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, month, year, when, col, month, regexp_replace, unix_timestamp
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField, DateType, DoubleType
from datetime import datetime

debug=1

# COMMAND ----------

# Install an Excel reader 
%pip install openpyxl

from pyspark.pandas import read_excel

# COMMAND ----------

# File location and type
input_file_location = GRE_MONTHLY_LOAD_PATH

# Get list of Excel files
input_files = dbutils.fs.ls(input_file_location)

if debug:
    display(input_files)


# COMMAND ----------

# Process Excel files into single dataframes for coincidental peak and monthly load info.
debug_for = 1

# Create DataFrame for the coincidental peak with specified schema
# Define the schema
schema = StructType([
    StructField("CoincidTime", DateType(), True),
    StructField("CoincidYear", IntegerType(), True),
    StructField("CoincidMonth", IntegerType(), True),
    StructField("CoincidDay", IntegerType(), True),
    StructField("CoincidHour", IntegerType(), True)
])
coincid_df = spark.createDataFrame([], schema)

for i in range(0, len(input_files)):
    this_file = input_files[i].path

    # Read the Excel file into a DataFrame.  The top rows are not needed.
    this_file_pdf = read_excel(input_files[i].path, sheet_name="Sheet1", header=0, dtype=str, skiprows=11)

    # Find the index of the row with 'Totals' in the first column and only keep rows above it.
    totals_index = this_file_pdf[this_file_pdf.iloc[:, 0] == 'TOTALS'].index.tolist()[0]
    this_file_pdf = this_file_pdf.iloc[:totals_index-1]
    
    # Extract the coincident peak from the remaining data.  This will also give us the month and year for the data.
    coincidental_peak = this_file_pdf.iloc[0, 5]
    if debug_for and i==0:
        print(coincidental_peak)

    # Convert to date time and get the time components.  Union with the coincidental peak dataframe.
    coincidental_peak_dt = datetime.strptime(coincidental_peak, '%Y-%m-%d %H:%M:%S')
    year = coincidental_peak_dt.year
    month = coincidental_peak_dt.month
    row_values = [coincidental_peak_dt, year, month, coincidental_peak_dt.day, coincidental_peak_dt.hour]
    coincid_df = coincid_df.union(spark.createDataFrame([row_values], coincid_df.columns))
    if debug_for and i==0:
        display(coincid_df)

    if debug_for and i==0:
        display(this_file_pdf)

    # Filter rows that have 'LR0' or 'IP' in the 'CONTRIBUTOR' column
    this_file_sub_pdf = this_file_pdf[this_file_pdf['CONTRIBUTOR'].str.startswith(('LR0', 'IP'))]

    if debug_for and i==0:
        display(this_file_sub_pdf)

    # Set the column names
    col_names = ['ID', 'Substation', 'CH', 'NonCoincidTime', 'NonCoincidLoad', 'CoincidLoad', 'GRESupplied', 'LDFact', 'PwrFact']
    this_file_sub_pdf.columns = col_names    

    # Create a spark dataframe for this round.
    this_file_cols_df = spark.createDataFrame(this_file_sub_pdf.values.tolist(), col_names)
    if debug_for and i==0:
        display(this_file_cols_df)

    # Add a placeholder row for contract lights and monthly adjustments.  The name of the substation is "NA".
    this_month_date = str(year) + "-" + f"{1:02d}" + "-01" + " 12:00"
    placeholder_row = ('Adjustments', 'NA', 0, this_month_date, 0, 0, 0, 0, 0)
    this_file_cols_df = this_file_cols_df.union(spark.createDataFrame([placeholder_row], col_names))

    this_file_final_df = this_file_cols_df.withColumn('Month', lit(month)) \
                                            .withColumn('Year', lit(year))

    if debug_for and i==0:
        display(this_file_final_df)

    if i==0:
        all_months_df = this_file_final_df
    else:
        all_months_df = all_months_df.union(this_file_final_df)


# COMMAND ----------

coincid_df = coincid_df.orderBy(col('CoincidTime'))
if debug:
    display(coincid_df)
    display(all_months_df)

# COMMAND ----------

# Save the coincidental load information to delta lake.
if debug:
    display(coincid_df)

coincid_df.write.mode('overwrite').format('delta').save(COINCIDENTAL_LOAD_SILVER_PATH)


# COMMAND ----------

# Rename the Fergus Ethanol plant to match iVUE.
all_months_df = all_months_df.withColumn('Substation', \
                                    when(all_months_df['Substation']!="FERGUS ETHANOL", all_months_df['Substation']) \
                                        .otherwise("FERGUS FALLS 2"))

if debug:
    display(all_months_df)

# COMMAND ----------

# Bring in the Erhard hybrid offset.  This is an adder to the GRE supplied.
erhard_location = ERHARD_HYBRID_PRODUCTION

erhard_df = spark.read.option("header", True).csv(erhard_location)

erhard_df = erhard_df.withColumn('Month', month(erhard_df.ENTERDATE)) \
                            .withColumn('Year', year(erhard_df.ENTERDATE)) \
                            .select('Month', 'Year', 'SUBSTATION', 'HYBRIDPRODUCTION' )

erhard_df = erhard_df.withColumnRenamed('SUBSTATION', 'Substation') \
                        .withColumnRenamed('HYBRIDPRODUCTION', 'HybridProduction')

if debug:
    display(erhard_df)

# COMMAND ----------

# Join the Erhard dataframe to the main dataframe.  
all_months_erhard_df = all_months_df.join(erhard_df, on=['Month', 'Year', 'Substation'], how='leftouter')

all_months_erhard_df = all_months_erhard_df.withColumn('TotalSupplied', \
                                    when(all_months_erhard_df['Substation']!="ERHARD", all_months_erhard_df['GRESupplied']) \
                                        .otherwise(all_months_erhard_df['GRESupplied']+all_months_erhard_df['HybridProduction']))

if debug:
    display(all_months_erhard_df)

# COMMAND ----------

# Do some data cleaning on the numeric columns and convert them.  

# Get rid of the alpha characters that show up in some columns.
all_months_erhard_clean_df = all_months_erhard_df.withColumn('CoincidLoad', regexp_replace('CoincidLoad', '[A-Z]', '')) \
                                            .withColumn('NonCoincidLoad', regexp_replace('NonCoincidLoad', '[A-Z]', ''))

if debug:
    display(all_months_erhard_clean_df)

# COMMAND ----------

# Convert the columns to proper data types.
all_months_types_df = all_months_erhard_clean_df \
            .withColumn('NonCoincidLoad', all_months_erhard_clean_df['NonCoincidLoad'].cast(DoubleType())) \
            .withColumn('CoincidLoad', all_months_erhard_clean_df['CoincidLoad'].cast(DoubleType())) \
            .withColumn('GRESupplied', all_months_erhard_clean_df['GRESupplied'].cast(DoubleType())) \
            .withColumn('LDFact', all_months_erhard_clean_df['LDFact'].cast(DoubleType())) \
            .withColumn('PwrFact', all_months_erhard_clean_df['PwrFact'].cast(DoubleType())) \
            .withColumn('HybridProduction', all_months_erhard_clean_df['HybridProduction'].cast(DoubleType()))\
            .withColumn('TotalSupplied', all_months_erhard_clean_df['TotalSupplied'].cast(DoubleType())) 

# Clean up nulls
all_months_types_df = all_months_types_df \
                    .withColumn('HybridProduction', when(all_months_types_df['HybridProduction'].isNull(), 
                                                 lit(0)).otherwise(all_months_types_df['HybridProduction'])) \


if debug:
    display(all_months_types_df)

# COMMAND ----------

# Add columns for Power BI join keys.
from pyspark.sql.functions import concat

all_months_types_df = all_months_types_df \
                        .withColumn('YearMonth', concat(all_months_types_df.Year, lit("-"), all_months_types_df.Month)) \
                        .withColumn('YearMonthSub', concat(all_months_types_df.Year, lit("-"), all_months_types_df.Month, lit("-"), all_months_types_df.Substation))

if debug:
    display(all_months_types_df)

# COMMAND ----------

# Save dataframe to delta lake.
all_months_types_df.write.mode('overwrite').format('delta').save(GRE_MONTHLY_LOAD_SILVER_PATH)


# COMMAND ----------

# Clean up the delta history.
spark.sql(f"VACUUM '{COINCIDENTAL_LOAD_SILVER_PATH}'")
spark.sql(f"VACUUM '{GRE_MONTHLY_LOAD_SILVER_PATH}'")

# COMMAND ----------

# Craete a table that can be used to enable a many to many relationship in Power BI.  This will be based on Year-Month-Substation.
# from pyspark.sql.functions import concat

# dim_df = all_months_erhard_df.select('Year', 'Month', 'Substation', 'YearMonthSub').distinct()

# if debug:
#     display(dim_df)


# COMMAND ----------

# Save dataframe to a single file.
# output_path = "/mnt/Silver/GREMonthlyLoad/DIM_YearMonthSub"
# output_file = "/DIM_YearMonthSub.csv"

# dest_file = output_path + output_file

# files = dbutils.fs.ls(output_path)

# # Remove old file if it exists.
# if len(files) != 0:
#     dbutils.fs.rm(dest_file)

# dim_df.coalesce(1).write.option("header",True).mode('overwrite').csv(output_path)

# files = dbutils.fs.ls(output_path)
# new_file = [x.path for x in files if x.path.endswith(".csv")][0]
# print(new_file)
# dbutils.fs.cp(new_file, dest_file)


