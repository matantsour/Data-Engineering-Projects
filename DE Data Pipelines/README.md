# Data Pipelines Project


## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Steps:
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

#### Fact and Dimension Operators
With the Fact and Dimension Operators, I leveraged an SQL helper class to perform data transformations. Most of the transformation logic is contained within SQL queries, and the operator executes these queries on the specified target database. Additionally, you can define a target table to store the transformation results.

Dimension loads typically follow the truncate-insert pattern, where the target table is cleared before loading new data. For this, I included a parameter to toggle between different insert modes during dimension loading. Fact tables, on the other hand, are often too large for such patterns and are generally designed for append-only operations.

#### Data Quality Operator
The final operator I implemented is the Data Quality Operator, which performs checks on the data itself. The operator takes one or more SQL test cases along with the expected results, executing the tests against the data.

For each test, the operator compares the actual result to the expected result. If there’s a mismatch, an exception is raised, causing the task to retry and eventually fail.

For instance, one test might query a column to check for NULL values by counting rows containing NULLs. Since we want to ensure there are no NULLs, the expected result would be 0, and the operator will compare the actual count with this expected value.
