# Exercise1-2: Simple Pipeline - Hello World Airflow!
# Task Flow API (Airflow 2.0) -> เป็นวิธีการเขียน DAG แบบใหม่ เหมาะกับ code ที่เป็น PythonOperator ทั้งหมด
# tutorial: https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html

# 1. Modules importing 
import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


# 2. Default arguments
default_args = {
    'owner': 'phu.pawitra',
}


# 4. Tasks
@task()
def print_hello():
    print("Hello World!")
    
@task()
def print_date():
    print(datetime.datetime.now())


# 3. Instantiate a DAG
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(1), tags=['practice-airflow'])
def exercise1_taskflow_dag():
    t1 = print_hello()
    t2 = print_date()
    # 5. Setting up Dependencies
    t1 >> t2


exercise1_dag = exercise1_taskflow_dag()
