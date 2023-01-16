# Exercise3: Fan-in Pipeline
# Task เยอะขึ้น & ใช้ DummyOperator เป็น task จำลอง

# 1. Modules importing 
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


# 2. Default arguments
default_args = {
    'owner': 'phu.pawitra',
}


# 3. Instantiate a DAG
with DAG(
    "exercise3_fan_in_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["practice-airflow"]
) as dag:

    
# 4. Tasks
    t = [ DummyOperator(task_id=f"task_{i}") for i in range(7)]

# 5. Setting up Dependencies
    [t[0],t[1],t[2]] >> t[4]
    [t[3],t[4],t[5]] >> t[6]