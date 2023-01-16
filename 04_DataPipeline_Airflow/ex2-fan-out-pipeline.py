# Exercise2: Fan-out Pipeline
# แยก pipeline ออกเพื่อให้ทำงานแบบ parallel พร้อมกัน & ใช้คำสั่ง gsutil จาก BashOperator

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
    "exercise2_fan_out_dag",
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

    t3 = BashOperator(
        task_id="bash_list",
        bash_command="gsutil ls gs://asia-east2-composer-de101-db090fe6-bucket/dags",
    )

    
# 5. Setting up Dependencies
# Run t3 พร้อมกับ t2 ได้
    t1 >> [t2,t3]
    
