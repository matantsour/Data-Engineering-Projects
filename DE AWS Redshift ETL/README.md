# Project: Data Warehouse

## Introduction
Sparkify, a music streaming startup, is migrating its data to the cloud.
The data, stored in S3, includes JSON logs of user activity and song metadata. 
In this project, I implemented an ETL pipeline that extracts data from S3,
stages it in Redshift, 
and transforms it into dimensional tables for analytics.

## Database Schema

### Staging Tables
- `staging_events`: User activity logs.
- `staging_songs`: Song metadata.

### Fact Table
- `songplays`: Records of song plays.

### Dimension Tables
- `users`: User data.
- `songs`: Song data.
- `artists`: Artist data.
- `time`: Timestamp breakdown.

## sql_queries.py:
- all the queries for creating the tables and dropping them are written in that file.

## create_tables.py:
- this file includes the commands for dropping the existing tables and creating them.

## ETL Pipeline
- `create_tables.py`: Drops and creates tables.
- `etl.py`: Copies data from S3 to Redshift and populates tables.


## How to run the project?
- Run the pipeline via `ProjectMain.ipynb`, which covers AWS setup, ETL execution, and cleanup.
