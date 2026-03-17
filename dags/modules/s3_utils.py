# dags/modules/s3_utils.py
import os
import json
import boto3

S3_BUCKET_NAME = "disclosure-insight-raw-data-ljh" 

def upload_to_s3(**context):
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    exec_date = context['logical_date'].strftime('%Y%m%d')
    ti = context['ti']
    disclosures = ti.xcom_pull(task_ids='fetch_dart_list')
    
    if not disclosures:
        print("💡 업로드할 공시 데이터가 없습니다.")
        return

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    file_key = f"raw/dart/{exec_date}/disclosure_list.json"
    
    json_data = json.dumps(disclosures, ensure_ascii=False, indent=4)
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=json_data.encode('utf-8'))
    print(f"✅ S3 업로드 완료: {file_key}")