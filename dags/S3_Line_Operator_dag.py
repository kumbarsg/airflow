from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from random import randint

from plugins.s3_line_extract_operator import S3LineExtractOperator

import logging

TARGET_BUCKET = "airflow-sgk-demo"
<<<<<<< HEAD
TARGET_KEY = "subfolder1/test1.txt"
=======
TARGET_KEY = "subfolder/test1.txt"
>>>>>>> 75bac1bd135e5b7d05d71cfe142d34aeb3ebf99e
LINE_NO = 3
USER_REGION = "us-east-1"

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(minutes=1),
    "edn_date": datetime.now() + timedelta(minutes=5),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
<<<<<<< HEAD
    "S3_Line_operator_dag",
=======
    "Line_operator_dag",
>>>>>>> 75bac1bd135e5b7d05d71cfe142d34aeb3ebf99e
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.now() - timedelta(minutes=1),
)


do_line_opr = S3LineExtractOperator(
    task_id="getLine",
    user_region=USER_REGION,
    target_bucket=TARGET_BUCKET,
    target_key=TARGET_KEY,
    line_no=LINE_NO,
    dag=dag,
)

do_line_opr
