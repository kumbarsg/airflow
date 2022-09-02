from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(minutes=1),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "create_dir_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.now() - timedelta(minutes=1),
)

# start = BashOperator(
#     task_id='start',
#     bash_command=""" echo 'hi' """,
#     dag=dag)

# stop = BashOperator(
#     task_id='stop',
#     bash_command="""  """,
#     dag=dag)

# LineExtractOperator(
# 	file_path= " /Users/chinnu/desktop/test.txt",
#     line_number : 2,
#     dag=dag
# )


# using python operator,
# open file - move to \n - copy the contents - display the copied stuff


# create_directory = BashOperator(
#     task_id='create_directory',
#     bash_command=""" mkdir /Users/chinnu/desktop/sample """,
#     dag=dag)

# touch_file = BashOperator(
#     task_id='create_file',
#     bash_command=""" touch /Users/chinnu/desktop/test.txt """,
#     dag=dag)

# move_file = BashOperator(
#     task_id='move_file',
#     bash_command=""" mv /Users/chinnu/desktop/test.txt /Users/chinnu/desktop/sample """,
#     dag=dag)

# create_directory.set_downstream(move_file)
# touch_file.set_downstream(move_file)
