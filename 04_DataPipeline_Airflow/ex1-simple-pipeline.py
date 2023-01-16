# Exercise1-1: Simple Pipeline - Hello World Airflow!
# PythonOperator, BashOperator, Task dependencies


# 1. Modules importing 
import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# 2. Default arguments
default_args = {
    'owner': 'phu.pawitra',
}

def print_func(something: str):
    print(something)


# 3. Instantiate a DAG
with DAG(
    "exercise1_simple_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["practice-airflow"]
) as dag:
  

# 4. Tasks
    t1 = PythonOperator(
        task_id="python_print",
        python_callable=print_func,
        op_kwargs={"something": "Hello World!"},
        )

    t2 = BashOperator(
        task_id="bash_print",
        bash_command="echo $(date)",
        )


# 5. Dependencies
    t1 >> t2
