# Practice Pipeline
# Read data from DB and API, Merge data and upload to GCS

# 1. Modules importing 
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests


# 2. Default arguments
default_args = {
    'owner': 'phu.pawitra',
}

MYSQL_CONNECTION = "mysql_practice_de"   # ชื่อของ connection ใน Airflow ที่เซ็ตเอาไว้
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

# output path
mysql_output_path = "/home/airflow/gcs/data/mysql_audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/api_conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/outputFromPipeline.csv"


def get_data_from_mysql(transaction_path):
    # รับ transaction_path มาจาก task ที่เรียกใช้

    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    # Merge data from 2 DataFrame
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

    # Save CSV file to transaction_path ("/home/airflow/gcs/data/audible_data_merged.csv") -> จะไปอยู่ที่ GCS โดยอัตโนมัติ
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


def get_conversion_rate(conversion_rate_path):
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)

    # Tranform index ที่เป็น date to column ชื่อ date แทน -> Save CSV file
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"Output to {conversion_rate_path}")


def merge_data(transaction_path, conversion_rate_path, output_path):
    # Read CSV file from path จากที่รับ parameter มา
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)

    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # merge 2 DataFrame
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
    # Transform price -> 1)เอาเครื่องหมาย $ ออก & 2)แปลงให้เป็น float
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)

    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop("date", axis=1)

    # Save CSV file
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
    print("== End of Pipeline ==")


# 3. Instantiate a DAG
with DAG(
    "exercise4_final_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["practice-pipeline"]
) as dag:

    dag.doc_md = """
    # Exercise4: Final DAG
    Read data from DB and API, Merge data and upload to GCS
    """
    

# 4. Tasks
    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )

    t2 = PythonOperator(
        task_id="get_data_from_api",
        python_callable=get_conversion_rate,
        op_kwargs={
            "conversion_rate_path": conversion_rate_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path,
            "output_path" : final_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path,
            "output_path" : final_output_path,
        },
    )


# 5. Dependencies
    [t1, t2] >> t3