# dags/modules/html_parser.py
import os
import json
import re
import time
import random
import requests
import boto3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

S3_BUCKET_NAME = "disclosure-insight-raw-data-ljh" 

def filter_and_extract_text(**context):
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    exec_date = context['logical_date'].strftime('%Y%m%d')
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    raw_file_key = f"raw/dart/{exec_date}/disclosure_list.json"
    
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=raw_file_key)
        raw_data = json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        print(f"📭 {exec_date} 원본 파일이 존재하지 않아 스킵합니다.")
        return [] 
    except Exception as e:
        raise e

    target_keywords = ['단일판매', '공급계약', '매출액또는손익구조', '배당', '주요사항보고서', '유상증자', '무상증자', '감자', '주식병합', '주식분할', '소송', '횡령', '배임', '영업정지']
    filtered_list = []
    
    for item in raw_data:
        if item.get('corp_cls') not in ['Y', 'K']:
            continue
        cleaned_title = re.sub(r'\[.*?\]|\(.*?\)', '', item.get('report_nm', '')).strip()
        if any(keyword in cleaned_title for keyword in target_keywords):
            item['clean_report_nm'] = cleaned_title
            filtered_list.append(item)
            
    print(f"🎯 필터링 완료: {len(filtered_list)}건 추출")

    parsed_data_list = []
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for item in filtered_list:
        rcept_no = item['rcept_no']
        try:
            print(f"📄 파싱 중... [{item['corp_name']}] {item['clean_report_nm']}")
            main_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
            res = session.get(main_url, verify=False, headers=headers, timeout=10)
            res.encoding = 'utf-8'
            
            all_matches = re.findall(r"viewDoc\s*\((.*?)\)", res.text)
            found_real_doc = False 
            
            for match_text in all_matches:
                args = re.findall(r"['\"](.*?)['\"]", match_text)
                if len(args) >= 2 and args[0].isdigit():
                    rcp_no, dcm_no = args[0], args[1]
                    ele_id = args[2] if len(args) > 2 else ""
                    offset = args[3] if len(args) > 3 else ""
                    length = args[4] if len(args) > 4 else ""
                    dtd = args[5] if len(args) > 5 else ""
                    
                    real_doc_url = f"https://dart.fss.or.kr/report/viewer.do?rcpNo={rcp_no}&dcmNo={dcm_no}&eleId={ele_id}&offset={offset}&length={length}&dtd={dtd}"
                    doc_res = session.get(real_doc_url, verify=False, headers=headers, timeout=10)
                    
                    soup = BeautifulSoup(doc_res.text, 'html.parser')
                    for script in soup(["script", "style"]):
                        script.extract()
                        
                    clean_text = re.sub(r'\s+', ' ', soup.get_text(separator=' ')).strip()
                    item['document_text'] = clean_text
                    found_real_doc = True
                    break 
                    
            if not found_real_doc:
                item['document_text'] = "원문 파싱 실패"
                
            parsed_data_list.append(item)
        except Exception as e:
            print(f"❌ 파싱 에러 ({rcept_no}): {e}")

        time.sleep(random.uniform(1.5, 3.5))

    processed_file_key = f"processed/dart/{exec_date}/parsed_disclosures.json"
    if parsed_data_list:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME, Key=processed_file_key,
            Body=json.dumps(parsed_data_list, ensure_ascii=False, indent=4).encode('utf-8')
        )
    return parsed_data_list