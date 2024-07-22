#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# Getting the current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Creating a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    file_path_list.sort()


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            


# In[4]:


#configuering a gialect for the csv data
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list: #pick conditions to filter upon
        if (row[0] == ''): 
            continue
        #pick columns you need to insert into the data models, those fields will be inserted into tables
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[5]:


# checking the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# In[6]:


#reading the new file - file to input to db
full_data_rows_list_new = []
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    csvreader = csv.reader(f) 
    next(csvreader)

    for line in csvreader:
        full_data_rows_list_new.append(line) 


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## The CSV file titled <font color=red>event_datafile_new.csv</font> is ready to work with and located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of how the denormalized data appears in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# #### Creating a Cluster

# In[7]:


from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect()


# #### Create Keyspace

# In[8]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[9]:


try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### With Apache Cassandra you model the database tables on the queries you want to run. <br> Now we need to create tables to run the following queries. 

# # We want to be able to answer 3 questions, for which we'll need to get the following data:
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# # For this purpose, we'll create 3 tables (each table is needed to answer each presented question and each table will be modeled accordingly):

# ### Table 1 - sessions_items <br>  Table 2 - users_sessions_items <br> Table 3 - songs

# ### In order to answer Q1 - <br> we'll create a table for which the key is composed of 2 fields: sessionId and itemInSession. <br > I chose this key because for Q1, the data needs to be filtered by sessoinId and itemInSession and in Apache Cassandra, we model our tables as per the queries we intend to run.<br>DataTypes for each field will be selected appropriately.

# In[10]:


#Table 1 - key (sessionId,itemInSession)
query = "DROP TABLE IF EXISTS sessions_items "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS sessions_items "
query = query + "(sessionId int,itemInSession int, artist text, song text, length float, PRIMARY KEY (sessionId, itemInSession) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# ### In order to answer Q2 - <br> we'll create a table with a composite partition key: <br> the userId, sessionId will serve as the partition key (we filter for Q2 based of those columns) and itemInSession will serve as the clustering column due to the fact the results need to be sorted as per this field's values. <br >DataTypes for each field will be selected appropriately.

# In[11]:


#Table 2 - key (userId,sesionId)
query = "DROP TABLE IF EXISTS users_sessions_items "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS users_sessions_items "
query = query + "(userId int,sessionId int,itemInSession int, artist text, song text, firstName text,lastName text, PRIMARY KEY ( (userId, sessionId), itemInSession) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# ### In order to answer Q3 - <br> we'll create a table for which the key is (song,userId). <br> This is chosen as the PK due to the fact that in Q3 we are requested to filter based on those columns. in Apache Cassandra we model the tables after the queries that fit the business needs.<br >DataTypes for each field will be selected appropriately.

# In[12]:


#Table 3 -song
query = "DROP TABLE IF EXISTS songs "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS songs "
query = query + "(song text, userId int, firstName text,lastName text, PRIMARY KEY (song,userId) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# ### Now that we have created our tables, we would like to insert actual data into those tables. <br> We will accomplish that by going over each row of the file (once) and excecute individual insert statment for each one of the three table created above.

# In[13]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        artist = line[0]
        firstName = line[1]
        itemInSession = int(line[3])
        lastName = line[4]
        length = float(line[5])
        sessionId = int(line[8])
        song = line[9]
        userId = int(line[10])
        
        #insert to table 1
        query1 = "INSERT INTO sessions_items (sessionId, itemInSession, artist, song,length)"
        query1 = query1 + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query1, (sessionId, itemInSession, artist, song,length) )
        
        query2 = "INSERT INTO users_sessions_items (userId ,sessionId ,itemInSession, artist, song , firstName ,lastName)"
        query2 = query2 + " VALUES (%s, %s, %s, %s,%s, %s, %s)"
        session.execute(query2, (userId ,sessionId ,itemInSession, artist, song , firstName ,lastName) )
        
        query3 = "INSERT INTO songs (song, userId, firstName, lastName)"
        query3 = query3 + " VALUES (%s, %s, %s, %s)"
        session.execute(query3, (song, userId, firstName, lastName)) 


# ## for each table, we will validate that data was in fact inserted into the table, and then we'll answer the asked question.

# #### We will now check that data was inserted into Table 1 - sessions_items

# In[14]:


query ="""
SELECT
sessionId, itemInSession, artist, song,length 
FROM sessions_items 
LIMIT 5
"""

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    row_vals = [row.sessionid, row.iteminsession, row.artist, row.song,row.length]
    print (" | ".join([str(x) for x in row_vals]))


# ### The question asked for table 1 - <br> 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

# In[15]:


query ="""
SELECT
artist,
song,
length 
FROM sessions_items 
WHERE sessionId = 338 and itemInSession = 4
"""

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    row_vals = [row.artist, row.song,row.length]
    print (" | ".join([str(x) for x in row_vals]))


# #### we can see that for the 338th session, the 4th item was the song "Music Matters" by Faithless, and its lengh is 495 seconds.

# #### We will now check that data was inserted into Table 2 - users_sessions_items

# In[16]:


query ="""
SELECT
userId,sessionId,itemInSession,artist,song,firstName,LastName
FROM users_sessions_items 
LIMIT 5
"""

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    row_vals = [row.userid, row.sessionid, row.iteminsession, row.artist,row.song,row.firstname,row.lastname]
    print (" | ".join([str(x) for x in row_vals]))


# ### The question asked for table 2 - <br> 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

# In[17]:


query ="""
SELECT
artist,song,firstName,LastName
FROM users_sessions_items 
WHERE userId = 10 and sessionId = 182
"""

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    row_vals = [row.artist,row.song,row.firstname,row.lastname]
    print (" | ".join([str(x) for x in row_vals]))


# #### We can see that for the user: id - 10 , first name Sylvie, last name Cruz, during session 182, she listened to 4 songs. None are of the same artist.

# #### We will now check that data was inserted into Table 3 - songs

# In[18]:


query ="""
SELECT
song,userId,firstName,lastName
FROM songs 
LIMIT 5
"""

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    row_vals = [row.song, row.userid, row.firstname, row.lastname]
    print (" | ".join([str(x) for x in row_vals]))


# ### The question asked for table 2 - <br>Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# In[19]:


query ="""
SELECT
firstName,lastName
FROM songs 
WHERE song  = 'All Hands Against His Own'
"""

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    row_vals = [row.firstname, row.lastname]
    print (" | ".join([str(x) for x in row_vals]))


# #### We can see that only 3 users have listened to the song "All Hands Against His Own".

# ### We will now drop the tables before closing out the sessions

# In[20]:


query = "DROP TABLE IF EXISTS sessions_items "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "DROP TABLE IF EXISTS users_sessions_items "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
#Table 3 - key (song)
query = "DROP TABLE IF EXISTS songs "
try:
    session.execute(query)
except Exception as e:
    print(e)


# ### We will now close the session and cluster connection¶

# In[21]:


session.shutdown()
cluster.shutdown()

