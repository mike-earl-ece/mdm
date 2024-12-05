# Script to create a file that serializes all intervals starting with 9/1/2021 and maps each serial index to date, time, and interval.
# When joined with peak events, this will ease the task of finding an overlap.  
# It could be used to simplify what is stored in the processed meter data.

#%%
# Create an index for n years.
import pandas as pd

start_date = "2023-01-01"
end_date = "2030-12-31"

#%%
from math import floor

def get_time_hour_from_interval(interval):
    
    hour = floor((interval)/4)
    
    increment = interval%4
    
    # Create dict for increment look up.
    intrvl_map = {0:":00", 1:":15", 2:":30", 3:":45"}
 
    # Concatenate hour:min string and return.
    time_stamp = str(hour).rjust(2,"0") + intrvl_map[increment]
    
    return(hour, time_stamp)  # Hour should represent hour ending

#%%
print(get_time_hour_from_interval(1))
print(get_time_hour_from_interval(3))
print(get_time_hour_from_interval(5))
print(get_time_hour_from_interval(93))
print(get_time_hour_from_interval(96))


#%%
# Create a daily data frame.
day_df = pd.DataFrame(columns=["Date", "TimeStamp", "Year", "Month", "Day", "Time", "Hour", "Minute", "Interval"], index=range(1,97))
for index, row in day_df.iterrows():
    row.Interval = index
    row.Hour, row.Time = get_time_hour_from_interval(index)

print(day_df)   

#%%
# Iterate over each date and create a new dataframe that has each interval for the day.

# Create a calendar dataframe for the target window.
calendar_df = pd.DataFrame({"Date": pd.date_range(start_date, end_date)})

# Create an empty target dataframe that will contain all the data.  In theory, this should be pre-allocated but not 
# sure if that will make much of a difference for this.
meter_calendar_df = pd.DataFrame(columns=["Date", "TimeStamp", "Year", "Month", "Day", "Time", "Hour","Minute", "Interval"])

#for i in range(0, calendar_df.shape[0]):
for index, date_row in calendar_df.iterrows():
    day_df.Date = date_row.Date.date()
    day_df.TimeStamp = str(date_row.Date.date()) + " " + day_df.Time
    day_df.Year = date_row.Date.year
    day_df.Month = date_row.Date.month
    day_df.Day = date_row.Date.day
    day_df.Hour = date_row.Date.hour
    day_df.Minute = date_row.Date.minute

    # Once the temp day_df is set up properly, append it to the target df.
    meter_calendar_df = pd.concat([meter_calendar_df, day_df]).reset_index(drop=True)

#%%
# Create the MeterSampleIndex as a serialized value starting with the first date.
meter_calendar_df['MeterSampleIndex'] = range(1,meter_calendar_df.shape[0]+1)


#%%
print(meter_calendar_df)

# %%
# Save as a CSV
meter_calendar_df.to_csv("Meter15Calendar.csv", index=False)
# %%
