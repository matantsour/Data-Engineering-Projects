# Data Pipelines Project


## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Steps:

## Data Source
Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

### Copy S3 Data
1-Create S3 bucket
2-Copy the data from the udacity bucket to the home cloudshell directory:
3-Copy the data from the home cloudshell directory to required bucket:

## AirFlowDAG
![DAG](https://github.com/matantsour/Data-Engineering-Projects/blob/main/DE%20Data%20Pipelines/images/dag.jpg)


## Project Structure
dags/data_pipeline_project.py: Directed Acyclic Graph definition with imports, tasks and task dependencies
plugins/helpers/sql_queries.py: Contains Insert SQL statements
dags/common/create_tables.sql: Contains SQL Table creations statements
plugins/operators/multi_sql_postgres.py: Operator that runs multiple sql statements to initialize the tables.
plugins/operators/stage_redshift.py: Operator that copies data from S3 buckets into redshift staging tables
plugins/operators/load_dimension.py: Operator that loads data from redshift staging tables into dimensional tables
plugins/operators/load_fact.py: Operator that loads data from redshift staging tables into fact table
plugins/operators/data_quality.py: Operator that validates data quality in redshift tables

## Execution
1. Create S3 Bucket and Copy data from source.
2. Add AWS connection info in Airflow via UI
3. Create Redshift serverless and connection information and store it in Airflow via UI
4. Run project DAG and monitor the execution via Airflow UI.

### Building the operators
To complete the project, you need to build four different operators to stage the data, transform the data, and run checks on data quality.

You can reuse the code from Project 2, but remember to utilize Airflow's built-in functionalities as connections and hooks as much as possible and let Airflow do all the heavy lifting when it is possible.

All of the operators and task instances will run SQL statements against the Redshift database. However, using parameters wisely will allow you to build flexible, reusable, and configurable operators you can later apply to many kinds of data pipelines with Redshift and with other databases.

#### Create the tables using custom Operator for excecuting multiple SQL Statements at once using PostgresHook
Due to a built in limitatation on PostgresOperator, you can't run multiple sql statements at the same time.
I solved this by added a custom operator to split an sql file to commands by ";". 
```
class MultiSQLPostgresOperator(BaseOperator):
    def __init__(self, postgres_conn_id, sql_file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_file_path = sql_file_path

    def execute(self, context):
        if not os.path.exists(self.sql_file_path):
            raise FileNotFoundError(f"SQL file not found: {self.sql_file_path}")
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with open(self.sql_file_path, 'r') as file:
            sql_statements = file.read().split(';')
            for statement in sql_statements:
                if statement.strip():
                    self.log.info(f"Executing: {statement.strip()}")
                    postgres_hook.run(statement.strip())
```

#### Stage Operator
The Stage Operator is designed to load JSON-formatted files from S3 into Amazon Redshift. It creates and executes a SQL COPY statement based on the given parameters, which define the S3 file location and the target Redshift table.

The parameters help identify the JSON files, and a key feature of the Stage Operator is its templated field, enabling it to load timestamped files from S3 according to the execution time, as well as to handle backfilling tasks.

We needed to make one stating operator to handle both the events data structure and the songs data structure.
This was quite complex due to 2 reasons:
1 - the event files (log files) needed to be formatted as json using a helper file "log_json_path.json":
```
{
    "jsonpaths": [
        "$['artist']",
        "$['auth']",
        "$['firstName']",
        "$['gender']",
        "$['itemInSession']",
        "$['lastName']",
        "$['length']",
        "$['level']",
        "$['location']",
        "$['method']",
        "$['page']",
        "$['registration']",
        "$['sessionId']",
        "$['song']",
        "$['status']",
        "$['ts']",
        "$['userAgent']",
        "$['userId']"
    ]
}
```

2- the songs data is stored in S3 but in the following folders format:
```
songs-data/*/*/*
``` 
the "*" represent letters in which each song name starts.
So the proper solution would be to change the stating operator to work in a manner of creating a manifest file of all the individual files that need to be copied from S3 to RedShift. 
That is because the regular Copy command of Redshift does not allow to specify multiple sources unless it's by the use of a manifest file. 
An example of such file:
```
{
  "entries": [
    {"url": "s3://de-course-data-pipelines13-project/song-data/A/A/A/TRAAAAK128F9318786.json", "mandatory": true},
    {"url": "s3://de-course-data-pipelines13-project/song-data/A/A/A/TRAAAAV128F421A322.json", "mandatory": true},
    {"url": "s3://de-course-data-pipelines13-project/song-data/A/A/A/TRAAABD128F429CF47.json", "mandatory": true}
  ]
}
```

#### Fact and Dimension Operators
With the Fact and Dimension Operators, I leveraged an SQL helper class to perform data transformations. Most of the transformation logic is contained within SQL queries, and the operator executes these queries on the specified target database. Additionally, you can define a target table to store the transformation results.

Dimension loads typically follow the truncate-insert pattern, where the target table is cleared before loading new data. For this, I included a parameter to toggle between different insert modes during dimension loading. Fact tables, on the other hand, are often too large for such patterns and are generally designed for append-only operations.

**using "helpers\sql_queries.py" file, I declared the insert statements to the various fact and dimension tables.**


#### Data Quality Operator
The final operator I implemented is the Data Quality Operator, which performs checks on the data itself. The operator takes one or more SQL test cases along with the expected results, executing the tests against the data.

For each test, the operator compares the actual result to the expected result. If there’s a mismatch, an exception is raised, causing the task to retry and eventually fail.

For instance, one test might query a column to check for NULL values by counting rows containing NULLs. Since we want to ensure there are no NULLs, the expected result would be 0, and the operator will compare the actual count with this expected value.

During DAG execution, the DataQualityOperator connects to the Redshift database using the specified connection ID.
For each table in the tables list:
It executes a SELECT COUNT(**) FROM table; query.
If the table is empty (COUNT(***) = 0), the operator raises a ValueError, failing the task.

## Thank you

This project is part of my learning path as a Data Engineer as part of Udacity Data Engineer with AWS course. 



## Project Rubric
![RUBRIC](https://github.com/matantsour/Data-Engineering-Projects/blob/main/DE%20Data%20Pipelines/images/rubric.jpg)
