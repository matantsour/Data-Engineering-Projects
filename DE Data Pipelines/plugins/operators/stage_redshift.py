import json
import boto3
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
        MANIFEST;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Get AWS credentials
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        s3 = boto3.client('s3',
                          aws_access_key_id=credentials.access_key,
                          aws_secret_access_key=credentials.secret_key)

        # List all files under the specified prefix
        self.log.info(f"Listing files in S3 bucket {self.s3_bucket} with prefix {self.s3_key}")
        response = s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_key)
        if 'Contents' not in response:
            raise FileNotFoundError(f"No files found for prefix {self.s3_key} in bucket {self.s3_bucket}")

        # Gather all file paths
        files = [f"s3://{self.s3_bucket}/{obj['Key']}" for obj in response['Contents']]

        # For testing purposes- reduce the amount of files to 10000
        files = files[:1000]
        if not files:
            raise FileNotFoundError(f"No files found matching prefix {self.s3_key}")

        # Create MANIFEST file content
        manifest_content = {"entries": [{"url": file, "mandatory": True} for file in files]}

        # Upload MANIFEST file to S3
        manifest_key = f"{self.s3_key}/manifest.json"
        manifest_path = f"s3://{self.s3_bucket}/{manifest_key}"
        self.log.info(f"Uploading MANIFEST file to {manifest_path}")
        s3.put_object(
            Bucket=self.s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest_content)
        )

        # Truncate the target table
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Truncating data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Copying the data
        # Execute the COPY command using the MANIFEST file
        self.log.info(f"Copying data to Redshift table {self.table} using MANIFEST file")

        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                manifest_path,
                credentials.access_key,
                credentials.secret_key,
                self.log_json_file
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                manifest_path,
                credentials.access_key,
                credentials.secret_key,
                'auto'
            )

        self.log.info(f"Executing COPY command: {formatted_sql}")
        redshift.run(formatted_sql)

        self.log.info(f"Successfully copied data to Redshift table: {self.table}")
