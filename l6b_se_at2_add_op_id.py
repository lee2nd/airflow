from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient 
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, # -1 在失敗時將不斷重試直到成功為止
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'L6B_SW_AT2_add_op_id',
    default_args=default_args,
    description='L6B_SW_AT2_add_op_id',
    schedule='*/10 * * * *', # 每 10 分鐘跑一次
)


def etl():

    # 資料庫連結
    client = MongoClient('mongodb://ivan:ivan@10.88.26.183:27017')
    db = client["AT"]
    collection_defectinfo = db["L6B_SW_AT2_defectinfo"]
    collection_charge2d = db["L6B_SW_AT2_charge2d"]

    result = collection_defectinfo.update_many(
        {},  # 空条件意味着匹配所有文档
        { 
            "$set": { 
                "op_id": "SW-CGL",      # 更新 op_id 字段
                "lot_id": "Dummy_LOTID" # 更新 lot_id 字段
            } 
        }
    )

    print(f"L6B_SW_AT2_defectinfo modified count: {result.modified_count}")       

    result = collection_charge2d.update_many(
        {},  # 空条件意味着匹配所有文档
        { 
            "$set": { 
                "op_id": "SW-CGL",      # 更新 op_id 字段
                "lot_id": "Dummy_LOTID" # 更新 lot_id 字段
            } 
        }
    )    

    print(f"L6B_SW_AT2_charge2d modified count: {result.modified_count}")    
    
    print("The current date and time is", datetime.now().strftime("%d/%m/%Y, %H:%M:%S"))
    client.close()
    print("==========Done==========")


task = PythonOperator(
    task_id='L6B_SW_AT2_add_op_id',
    python_callable=etl,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)


task
