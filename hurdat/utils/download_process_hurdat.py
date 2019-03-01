import pandas as pd
import numpy as np
import os
import datetime

#The header information needs to be explicitly defined using the following code.
headers = ['date','time','record_id','status','latitude','longitude','max_wind','min_pressure',
           'ne34ktr','se34ktr','sw34ktr','nw34ktr','ne50ktr','se50ktr','sw50ktr','nw50ktr',
           'ne64ktr','se64ktr','sw64ktr','nw64ktr']

#if file isn't downloaded, get it from NHC and define headers.  Save as raw file with no indexes
if not os.path.isfile("../data/hurdat_raw.csv"):
    url = 'https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2017-050118.txt'
    df = pd.read_csv(url, names=headers)
    df.to_csv("../data/hurdat_raw.csv", index=False)
else:
    df = pd.read_csv("../data/hurdat_raw.csv")
    
#Initialize new columns
df['identifier'] = np.nan
df['name'] = np.nan
df['num_pts'] = np.nan

#Parse the new column data from the unique storm identifier rows
identifiers = df[[x[:2].isalpha() for x in df.date.values]]['date']
name = df[[x[:2].isalpha() for x in df.date.values]]['time']
num_pts = df[[x[:2].isalpha() for x in df.date.values]]['record_id']

#Place the parsed data into only the rows that are unique identifier rows
df.loc[[x[:2].isalpha() for x in df.date.values], 'identifier'] = identifiers
df.loc[[x[:2].isalpha() for x in df.date.values], 'name'] = name
df.loc[[x[:2].isalpha() for x in df.date.values], 'num_pts'] = num_pts

#Forward fill the unique identifier information into the NaN rows
df['identifier'].fillna(method='ffill', inplace=True)
df['name'].fillna(method='ffill', inplace=True)
df['num_pts'].fillna(method='ffill', inplace=True)

#Get a copy of only those rows that are not unique identifier rows
df_storms = df[[x[:2].isnumeric() for x in df.date.values]].copy()

#parse year, month, day, and hour from the date column
yr = [int(x[:4]) for x in df_storms.date.values]
mo = [int(x[4:6]) for x in df_storms.date.values]
dy = [int(x[6:8]) for x in df_storms.date.values]
hr = [int(x[:3]) for x in df_storms.time.values]

#set these values for new columns
df_storms['year'] = yr
df_storms['month'] = mo
df_storms['day'] = dy
df_storms['hour'] = hr

#Create a datetime column
df_storms['datetime'] = pd.to_datetime(df_storms[['year', 'month', 'day', 'hour']])

#Reorder the columns
df_storms = df_storms[['identifier', 'name', 'num_pts', 'year', 'month',
                       'day', 'hour', 'datetime', 'date', 'time', 'record_id', 
                       'status', 'latitude', 'longitude', 'max_wind', 
                       'min_pressure', 'ne34ktr', 'se34ktr', 'sw34ktr', 'nw34ktr',
                       'ne50ktr', 'se50ktr', 'sw50ktr', 'nw50ktr', 'ne64ktr', 'se64ktr',
                       'sw64ktr', 'nw64ktr']]
 
#Remove whitespace from some columns
df_storms['name'] = df_storms['name'].apply(lambda x: x.strip())
df_storms['num_pts'] = df_storms['num_pts'].apply(lambda x: x.strip())
df_storms['status'] = df_storms['status'].apply(lambda x: x.strip())
df_storms['latitude'] = df_storms['latitude'].apply(lambda x: x.strip())
df_storms['longitude'] = df_storms['longitude'].apply(lambda x: x.strip())

#Take the lat and lon data and turn them into floats
is_south = [x[-1]=='S' for x in df_storms['latitude']]
is_west = [x[-1]=='W' for x in df_storms['longitude']]
df_storms['lat_f'] = [float(x[:-1]) for x in df_storms['latitude']]
df_storms['lon_f'] = [float(x[:-1]) for x in df_storms['longitude']]
df_storms.loc[is_south, 'lat_f'] *= -1
df_storms.loc[is_west, 'lon_f'] *= -1

#Provide bools for basic logical tests
df_storms['TROP'] = (df_storms.status=='HU') | (df_storms.status=='TS')
df_storms['TS'] = df_storms.max_wind >= 34
df_storms['CAT1'] = df_storms.max_wind >= 64
df_storms['CAT2'] = df_storms.max_wind >= 83
df_storms['CAT3'] = df_storms.max_wind >= 96
df_storms['CAT4'] = df_storms.max_wind >= 113
df_storms['CAT5'] = df_storms.max_wind >= 137

#save the preprocessed data as a csv
df_storms.to_csv("../data/hurdat2_cleaned.csv")