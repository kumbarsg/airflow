from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from random import randint


import logging

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
    "airflow_exercise_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.now() - timedelta(minutes=1),
)

#hi ssup
# ---------------python opr logic for line opr----------------
# class LineOperator(BaseOperator):
#     def __init__(self,name: str, **kwargs) -> None:
#         super().__init__(**kwargs)
#         self.name = name

#     def execute():
#         lines = []
#         with open('/Users/chinnu/desktop/sample/test.txt') as f:
#             lines = f.readlines()
#         print(lines[1])
#     #context['ti'].xcom_push(key='xyz', value=lines[1])
#         f.close()
#         return lines[1]

# ---------------python opr logic for line opr----------------


# ---------------python logic for line opr----------------

# def python_start():
#     pass

# def python_show(**context):
#     value = context['ti'].xcom_pull(task_ids=python_fun_task.task_id, key='xyz')
#     print(value)

# def python_fun(**context):
#     lines = []
#     with open('/Users/chinnu/desktop/sample/test.txt') as f:
#         lines = f.readlines()
#     print(lines[1])
#     context['ti'].xcom_push(key='xyz', value=lines[1])
#     f.close()

# python_pull_task = PythonOperator(
#     task_id="python_pull_task",
#     python_callable=python_show,
#     provide_context=True,
#     dag=dag)

hi

# python_fun_task = PythonOperator(
#     task_id="python_file_task",
#     python_callable=python_fun,
#     provide_context=True,
#     dag=dag)

# python_start_task = PythonOperator(
#     task_id="python_start_task",
#     python_callable=python_start,
#     provide_context=True,
#     dag=dag )

# python_fun_task.set_upstream(python_start_task)
# python_pull_task.set_upstream(python_fun_task)

# ------------ python dag n logic end for line opr--------------


# -------content passing refs--------------------------------------------
# def python_push_function(**context):
#     value = "are you working?"
#     logging.info("value: " + value)
#     context['ti'].xcom_push(key='xyz', value=value)

# python_push_task = PythonOperator(
#     task_id="python_push_task",
#     python_callable=python_push_function,
#     provide_context=True,
#     dag=dag)


# def python_pull_function(**context):
#     value = context['ti'].xcom_pull(task_ids=python_push_task.task_id, key='xyz')
#     logging.info("value: " + value)

# python_pull_task = PythonOperator(
#     task_id="python_pull_task",
#     python_callable=python_pull_function,
#     provide_context=True,
#     dag=dag)

# python_push_task.set_downstream(python_pull_task)
# -----------------------------------------------------------------------
