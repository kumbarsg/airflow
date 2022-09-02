from datetime import datetime
from os import getenv
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue import AwsGlueJobSensor
from airflow.providers.amazon.aws.sensors.glue_crawler import AwsGlueCrawlerSensor

import boto3

GLUE_DATABASE_NAME = "crime_sample"

GLUE_EXAMPLE_S3_BUCKET = "s3://airflow-sgk-demo/glue"

GLUE_CRAWLER_ROLE = "arn:aws:iam::197183385700:role/sgk-poc-glue-default"

# Role needs putobject/getobject access to the above bucket as well as the glue
# service role, see docs here: https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html

s3_ingest_script_path = "s3://airflow-sgk-demo/glue/Scripts/glue_spark_ingest.py"


with DAG(
    dag_id="example_glue",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as glue_dag:

    job_name = "glue_job"
    submit_glue_job = AwsGlueJobOperator(
        task_id="glue_job",
        job_name=job_name,
        # num_of_dpus=10,
        retry_limit=0,
        script_location=f"s3://airflow-sgk-demo/glue/Scripts/glue_spark_ingest.py",
        s3_bucket=GLUE_EXAMPLE_S3_BUCKET,
        iam_role_name=GLUE_CRAWLER_ROLE.split("/")[-1],
        create_job_kwargs={
            'GlueVersion': '3.0', 
            # 'NumberOfWorkers': 2, 'WorkerType': 'G.1X',
            # 'AllocatedCapacity': 0, 
            "DefaultArguments": {"--enable-glue-datacatalog": ''," ss": " "}
        },
        #  Name, Description, LogUri, Role, ExecutionProperty, Command,
        # DefaultArguments, NonOverridableArguments, Connections, MaxRetries, AllocatedCapacity, Timeout, MaxCapacity, SecurityConfiguration, Tags, NotificationProperty, GlueVersion, NumberOfWorkers, WorkerType
        #     run_job_kwargs={'GlueVersion': '3.0'}
        #     #JobName, JobRunId,
        #
        # Arguments, AllocatedCapacity, Timeout, MaxCapacity, SecurityConfiguration, NotificationProperty, WorkerType, NumberOfWorkers
    )
    

    chain(submit_glue_job)
