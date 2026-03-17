# dags/modules/db_utils.py
import os
import json
import boto3
import pandas as pd
import psycopg2
import psycopg2.extras

S3_BUCKET_NAME = "disclosure-insight-raw-data-ljh" 

def load_to_postgres(**context):
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    exec_date = context['logical_date'].strftime('%Y%m%d')
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    final_file_key = f"final/dart/{exec_date}/structured_disclosures.json"
    
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=final_file_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"❌ S3 파일 읽기 실패 (또는 대상 없음): {e}")
        return

    if not data:
        print("💡 적재할 데이터가 없습니다.")
        return

    print(f"🚚 DB 벌크 인서트(UPSERT) 준비 중... (총 {len(data)}건)")

    df = pd.DataFrame(data)
    if 'extracted_data' in df.columns:
        df['extracted_data'] = df['extracted_data'].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else None
        )

    try:
        conn = psycopg2.connect(
            host="postgres-dw",
            port=5432,
            dbname="finance_dw",
            user="dw_user",
            password="dw_password"
        )
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dart_disclosures (
            corp_code VARCHAR(50),
            corp_name VARCHAR(100),
            stock_code VARCHAR(50),
            corp_cls VARCHAR(10),
            report_nm VARCHAR(255),
            rcept_no VARCHAR(50) PRIMARY KEY,
            flr_nm VARCHAR(100),
            rcept_dt VARCHAR(50),
            rm VARCHAR(50),
            clean_report_nm VARCHAR(255),
            extracted_data JSONB
        );
        """
        cursor.execute(create_table_query)
        
        columns = df.columns.tolist()
        values = [tuple(row) for row in df.to_numpy()] 
        
        insert_query = f"""
            INSERT INTO dart_disclosures ({','.join(columns)}) 
            VALUES %s 
            ON CONFLICT (rcept_no) 
            DO UPDATE SET 
                extracted_data = EXCLUDED.extracted_data,
                clean_report_nm = EXCLUDED.clean_report_nm
        """
        
        psycopg2.extras.execute_values(cursor, insert_query, values)
        conn.commit() 
        print(f"🎉 DB 적재 완료! 중복 없이 성공적으로 {len(df)}건이 'dart_disclosures' 테이블에 저장되었습니다.")
        
    except Exception as e:
        print(f"❌ DB 적재 에러 발생: {e}")
        
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()