from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from cosmos import DbtTaskGroup
from airflow.utils.dates import days_ago 

import requests
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'retries': 2
}


with DAG(
    'api_to_redshift__with_dbt_pipeline',
    default_args=default_args,
    description='ETL pipeline from API to AWS s3 and Redshift, with dbt transformations',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['example'],
) as dag:

    @task()
    def extract_data():
        """Extract data from a public API"""
        url = 'https://api.example.com/data'
        response = requests.get(url)
        response.raise_for_status() # Raise an error if the HTTP request returned an unsuccessful status code
        data = response.json()

        return data
    
    @task()
    def transform_data(data):
        """Transform the extracted data"""
        # Example: convert JSON to DataFrame and do light transformations
        df = pd.DataFrame(data)
        # Add or modify columns as needed
        df['processed_timestamp'] = pd.Timestamp.now()

        return df.to_dict(orient='records')


    @task()
    def load_data_to_s3(transformed_data):
        """Load the transformed data to AWS S3 as CSV"""
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_bucket = 'your-s3-bucket-name'
        file_name = 'data/transformed_data.csv'

        # convert transformed data to csv string
        df = pd.DataFrame(transformed_data)

        # Save DataFrame to CSV in-memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload CSV to S#
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=file_name,
            bucket_name=s3_bucket,
            file_format='csv',
            replace=True
            )
        
        return file_name

    @task()
    def load_data_to_redshift(file_name):
        """Load the data from S3 to Redshift"""
        
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        s3_path = f's3://your-s3-bucket/{file_name}'
        redshift_table = 'public.api_data' # Replace with your Redshift table name 

        # SQL Query to copy data from s3 to Redshift
        copy_sql = f"""
            COPY {redshift_table}
            FROM '{s3_path}'
            IAM_ROLE 'your-redshift-iam-role'
            FORMAT AS CSV
            IGNOREHEADER 1;
        """

        #Execute the copy command
        redshift_hook.run(copy_sql)

    dbt_group = DbtTaskGroup(
        group_id = "customers_group",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH / "jaffle_shop").as_posix(),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        ),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 2},
    )
    