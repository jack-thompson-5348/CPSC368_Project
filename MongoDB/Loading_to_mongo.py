#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing libraires
import pymongo
import pandas as pd


# In[2]:


#Loading cleaned data
data_2022 = pd.read_csv("../CleanedData/cleaned_data_2022.csv")
data_2023 = pd.read_csv("../CleanedData/cleaned_data_2023.csv")
data_2024 = pd.read_csv("../CleanedData/cleaned_data_2024.csv")
#Checking number of observations in each dataset
print(f"data_2022 has {len(data_2022)} observations")
print(f"data_2023 has {len(data_2023)} observations")
print(f"data_2024 has {len(data_2024)} observations")


# In[3]:


#Establishing connection to MongoDB
CWL = 'jthomp20'
SNUM = '84340637'
connection_string = f"mongodb://{CWL}:a{SNUM}@localhost:27017/{CWL}"
client = pymongo.MongoClient(connection_string)
db = client[CWL]["songs"]


# In[4]:


#Creating empty array to which we will append
songs_to_insert = []


# In[5]:


#Function to split artists into an array (stored in data as 1 string)
def split_artists(val):
    return [a.strip() for a in str(val).split(',')]


# In[6]:


#Adding observations from data_2022 to songs_to_insert (streams not in data file)
for _, row in data_2022.iterrows():
    songs_to_insert.append({
        "year": 2022,
        "track_name": row['track_name'],
        "artist_names": split_artists(row['artist_names']),
        "metrics": {
            "peak_rank": row['peak_rank'],
            "weeks_on_chart": row['weeks_on_chart'],
            "streams": None
        }
    })


# In[7]:


#Adding observations from data_2023 to songs_to_insert (peak_rank and weeks_on_chart not in data file)
for _, row in data_2023.iterrows():
    songs_to_insert.append({
        "year": 2023,
        "track_name": row['track_name'],
        "artist_names": split_artists(row['artist_names']),
        "metrics": {
            "peak_rank": None,
            "weeks_on_chart": None,
            "streams": row['streams']
        }
    })


# In[8]:


#Adding observations from data_2024 to songs_to_insert (weeks_on_chart not in data file, artist_names is only one artist, other artist in track_name)
for _, row in data_2024.iterrows():
    songs_to_insert.append({
        "year": 2024,
        "track_name": row['track_name'],
        "artist_names": split_artists(row['artist_names']),
        "metrics": {
            "peak_rank": row['all_time_rank'],
            "weeks_on_chart": None,
            "streams": row['spotify_streams']
        }
    })


# In[9]:

db.delete_many({})
#inserting songs_to_insert into MongoDB
if songs_to_insert:
    result = db.insert_many(songs_to_insert)


# In[13]:


f"Length of songs_to_insert is {len(songs_to_insert)}"


# In[14]:


f"Length of data_2022, data_2023 and data_2024 combined is {len(data_2022) + len(data_2023) + len(data_2024)}"

