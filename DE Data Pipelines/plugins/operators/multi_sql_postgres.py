from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

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