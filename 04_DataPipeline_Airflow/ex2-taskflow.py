import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'phu.pawitra',
}

@task()
def print_hello():
    print("Hello World!")
    

@task()
def print_date():
    print(datetime.datetime.now())


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(1), tags=['practice-airflow'])
def exercise2_taskflow_dag():
    t1 = print_hello()
    t2 = print_date()
    t3 = BashOperator(
        task_id="list_file_gcs",
        bash_command="gsutil ls gs://asia-east2-composer-de101-db090fe6-bucket/dags"
    )
    
    t1 >> [t2, t3]

exercise2_dag = exercise2_taskflow_dag()
