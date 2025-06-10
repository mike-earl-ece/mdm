# Databricks notebook source
# MAGIC %md
# MAGIC ### Config Utilities
# MAGIC Functions and constants that suppport reuse of key configuration information.

# COMMAND ----------

# Constants

# Storage
CONTAINER_URI_PATH = "abfss://meter-data@ecemdmstore.dfs.core.windows.net/"  # Production
#CONTAINER_URI_PATH = "abfss://meter-data-test@ecemdmstore.dfs.core.windows.net/"  # Test

# Schema - used for pathing below.
SCHEMA = "default"  # Production
#SCHEMA = "test"   # Test

# Ingestion
DEMAND_RATE_PATH = CONTAINER_URI_PATH + "Bronze/Misc/DemandRates/GREDemandCharges.csv"
DIM_METER_INFO_PATH = CONTAINER_URI_PATH + "Bronze/iVUE/MeterInfo/DIM_MeterInfo.csv"
MDM_LANDING_PATH = CONTAINER_URI_PATH + "MDMLandingZone" 
MDM_INGEST_CHECKPOINT_PATH = CONTAINER_URI_PATH + "Bronze/Checkpoint"
MDM_INGEST_TABLE = SCHEMA + ".bronze_mdmingest"
METER_CONTROL_TYPES_PATH = CONTAINER_URI_PATH + "Bronze/iVUE/MeterLoadControlTypes/MeterLoadControlTypes.csv"

# Bronze to SilverConformed
COINCIDENTAL_LOAD_INDEX_PATH = CONTAINER_URI_PATH + "SilverConformed/CoincidentalLoad/MonthlyIndex/"
DIM_SUBMETER_MAP_PATH = CONTAINER_URI_PATH + "SilverConformed/DIMSubmeterMap"
MDM_CLEANED_PATH = CONTAINER_URI_PATH + "SilverConformed/MDM/Cleaned/"
MDM_CLEANED_TABLE = SCHEMA + ".silverconformed_mdm_cleaned"
MDM_INDEXED_PATH = CONTAINER_URI_PATH + "SilverConformed/MDM/Indexed/"
MDM_INDEXED_TABLE = SCHEMA + ".silverconformed_mdm_indexed"
INDEXED_CALENDAR_PATH = CONTAINER_URI_PATH + "SilverConformed/IndexedCalendar/"

# SilverConformed to SilverEnhanced
MDM_HOURLY_NO_SUBMETERS_PATH = CONTAINER_URI_PATH + "SilverEnhanced/MDM/Hourly_NoSubmeters/"
MDM_HOURLY_PATH = CONTAINER_URI_PATH + "SilverEnhanced/MDM/Hourly/"
MDM_NO_SUBMETERS = CONTAINER_URI_PATH + "SilverEnhanced/MDM/NoSubmeters/"

# Silver to Gold
COINCIDENTAL_PEAK_ALL_METER_TOTAL_PATH = CONTAINER_URI_PATH + "Gold/LoadControl/CoincidPeakAllMeterTotal"
COINCIDENTAL_PEAK_DIM_YEARMONTH_PATH = CONTAINER_URI_PATH + "Gold/LoadControl/DIMYearMonth"
COINCIDENTAL_PEAK_LOADCONTROL_USAGE_PATH = CONTAINER_URI_PATH + "Gold/LoadControl/CoincidPeakLCUsage"
MDM_DAILY_RANK_PATH = CONTAINER_URI_PATH + "Gold/MDM/DailyRank/"
MDM_DIAGNOSTICS_PATH = CONTAINER_URI_PATH + "Gold/MDM/Diagnostics/"
MDM_MONTH_HOUR_RANK_PATH = CONTAINER_URI_PATH + "Gold/MDM/MonthHourRank/"
MDM_HOURLY_TOTALS_PATH = CONTAINER_URI_PATH + "Gold/MDM/HourlyTotals/"


# COMMAND ----------

def set_spark_config():
    storage_end_point = "ecemdmstore.dfs.core.windows.net" 
    my_scope = "MeterDataStorageVault"
    my_key = "MeterStorageAccessKey"

    spark.conf.set(
        "fs.azure.account.key." + storage_end_point,
        dbutils.secrets.get(scope=my_scope, key=my_key))
