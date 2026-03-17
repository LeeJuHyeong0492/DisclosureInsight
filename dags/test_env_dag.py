import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_env_variables():
    """
    Airflow 컨테이너 내부에 환경변수가 잘 들어왔는지 출력해 봅니다.
    보안을 위해 API 키의 전체를 출력하지 않고 앞/뒤 일부만 출력합니다.
    """
    print("🔍 [환경변수 체크 시작]")
    
    # 1. 잘 작동했던 DART API 키 확인 (비교군)
    dart_key = os.getenv("DART_API_KEY")
    if dart_key:
        print(f"✅ DART_API_KEY: 설정됨 (길이: {len(dart_key)})")
    else:
        print("❌ DART_API_KEY: None (설정 안 됨)")

    # 2. 문제가 된 OpenAI API 키 확인 (실험군)
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        # 키가 길 경우 앞 7자리, 뒤 4자리만 출력하여 안전하게 확인
        masked_key = f"{openai_key[:7]}...{openai_key[-4:]}" if len(openai_key) > 15 else "***"
        print(f"✅ OPENAI_API_KEY: 설정됨 -> {masked_key}")
    else:
        print("❌ OPENAI_API_KEY: None (환경변수가 컨테이너로 전달되지 않았습니다!!)")
        
    print("🔍 [환경변수 체크 종료]")

# 간단하게 수동으로만 실행할 수 있는 DAG
with DAG(
    dag_id='test_env_variables',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # 스케줄 없이 수동 실행
    catchup=False,
    tags=['Debug'],
) as dag:

    check_env_task = PythonOperator(
        task_id='check_env_task',
        python_callable=check_env_variables,
    )