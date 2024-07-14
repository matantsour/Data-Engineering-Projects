#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[71]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[72]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)
    file_path_list.sort()


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[73]:


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
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables


# In[74]:


for row in full_data_rows_list:
    pass


# In[75]:


csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list: #pick conditions to filter upon
        if (row[0] == ''): 
            continue
        #pick columns you need
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[76]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# In[77]:


#reading the new file - file to input to db
full_data_rows_list_new = []
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    # creating a csv reader object 
    csvreader = csv.reader(f) 
    next(csvreader)

# extracting each data row one by one and append it        
    for line in csvreader:
        #print(line)
        full_data_rows_list_new.append(line) 


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
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
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[78]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[79]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[80]:


try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# In[81]:


#create appropriate tables (if not exists) to answer the following 3 questions:
### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


# In[82]:


fields = ['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId']
l = list(enumerate(fields))


# In[83]:


c=0
for v in full_data_rows_list_new[0]:
    l[c] = tuple((l[c],v))
    c+=1


# In[84]:


l


# In[85]:


#Table 1 - key (sessionId,itemInSession)
query = "DROP TABLE IF EXISTS sessions_items "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS sessions_items "
query = query + "(sessionId text,itemInSession text, artist text, song text, length text, PRIMARY KEY (sessionId, itemInSession) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# In[86]:


#Table 2 - key (userId,sesionId)
query = "DROP TABLE IF EXISTS users_sessions_items "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS users_sessions_items "
query = query + "(userId text,sessionId text,itemInSession text, artist text, song text, firstName text,lastName text, PRIMARY KEY (userId,sessionId,itemInSession) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# In[87]:


#Table 3 - key (song)
query = "DROP TABLE IF EXISTS songs "
try:
    session.execute(query)
except Exception as e:
    print(e)
    
query = "CREATE TABLE IF NOT EXISTS songs "
query = query + "(song text, userId text, firstName text,lastName text, PRIMARY KEY (song,userId) )"
try:
    session.execute(query)
except Exception as e:
    print(e)


# In[88]:


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        #insert to table 1
        query1 = "INSERT INTO sessions_items (sessionId, itemInSession, artist, song,length)"
        query1 = query1 + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query1, (line[8], line[3],line[0], line[9], line[5]) )
        
        query2 = "INSERT INTO users_sessions_items (userId ,sessionId ,itemInSession, artist, song , firstName ,lastName)"
        query2 = query2 + " VALUES (%s, %s, %s, %s,%s, %s, %s)"
        session.execute(query2, (line[10], line[8],line[3], line[0],line[9], line[1], line[4]) )
        
        query3 = "INSERT INTO songs (song, userId, firstName, lastName)"
        query3 = query3 + " VALUES (%s, %s, %s, %s)"
        session.execute(query3, (line[9], line[10],line[1], line[4]) )


# #### Do a SELECT to verify that the data have been inserted into each table

# In[89]:


query = "select * from sessions_items LIMIT 10"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.sessionid, row.iteminsession, row.artist, row.song,row.length)


# In[90]:


query = "select * from users_sessions_items LIMIT 10"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.userid, row.sessionid, row.iteminsession, row.artist,row.song,row.firstname,row.lastname)


# In[91]:


query = "select * from songs LIMIT 10"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.song, row.userid, row.firstname, row.lastname)


# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[92]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4


query = "select * from sessions_items where sessionId = '338' and itemInSession ='4'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song,row.length)                    


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[93]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

query = "select * from users_sessions_items where userId = '10' and sessionId = '182' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist, row.song, row.iteminsession,row.firstname,row.lastname)

                    


# In[94]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


query = "select * from songs where song = 'All Hands Against His Own' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.firstname, row.lastname)                    


# ### Drop the tables before closing out the sessions

# In[95]:


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


# ### Close the session and cluster connection¶

# In[96]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




