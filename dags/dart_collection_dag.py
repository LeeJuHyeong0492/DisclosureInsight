# dags/dart_collection_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.dart_api import fetch_dart_list
from modules.s3_utils import upload_to_s3
from modules.html_parser import filter_and_extract_text
from modules.llm_agent import transform_with_llm
from modules.db_utils import load_to_postgres

# DAG 기본 설정
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='dart_disclosure_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 18 * * 1-5', # 평일 오후 6시 실행
    catchup=False,
    max_active_runs=1,
    tags=['DART', 'Finance', 'Agentic Workflow'],
) as dag:

    # ==========================================================
    # Task 정의
    # ==========================================================
    task_fetch = PythonOperator(
        task_id='fetch_dart_list',
        python_callable=fetch_dart_list
    )
    
    task_upload_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )
    
    task_extract_text = PythonOperator(
        task_id='filter_and_extract_text',
        python_callable=filter_and_extract_text
    )
    
    task_transform_llm = PythonOperator(
        task_id='transform_with_llm',
        python_callable=transform_with_llm
    )
    
    task_load_db = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )
    
    # ==========================================================
    # 파이프라인 흐름 (Dependencies)
    # ==========================================================
    task_fetch >> task_upload_s3 >> task_extract_text >> task_transform_llm >> task_load_db