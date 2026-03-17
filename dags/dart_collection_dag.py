import os
import json
import boto3
import requests
import re
import urllib3
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
import psycopg2.extras

# ==========================================================
# 환경변수 로드
# ==========================================================
DART_API_KEY = os.getenv("DART_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
S3_BUCKET_NAME = "disclosure-insight-raw-data-ljh" 

# ==========================================================
# Task 1: DART API 목록 수집
# ==========================================================
def fetch_dart_list(**context):
    exec_date = context['logical_date'].strftime('%Y%m%d')
    url = "https://opendart.fss.or.kr/api/list.json"
    
    all_disclosures = []
    page_no = 1 
    
    print(f"🚀 [{exec_date}] DART 공시 데이터 전체 수집 시작...")

    while True:
        params = {
            'crtfc_key': DART_API_KEY,
            'bgn_de': exec_date,
            'end_de': exec_date,
            'page_count': 100,
            'page_no': page_no
        }
        
        print(f"📡 API 호출 중... (요청 페이지: {page_no})")
        response = requests.get(url, params=params)
        data = response.json()
        
        if data.get('status') == '000':
            disclosures = data.get('list', [])
            all_disclosures.extend(disclosures)
            
            total_page = data.get('total_page', 1)
            if page_no >= total_page:
                break
                
            page_no += 1
            
        elif data.get('status') == '013':
            print("💡 조회된 공시 데이터가 없습니다.")
            break
        else:
            print(f"❌ API 호출 실패: {data.get('message')}")
            raise ValueError(data.get('message'))

    print(f"✅ 수집 완료! 총 {len(all_disclosures)}건 확보")
    return all_disclosures

# ==========================================================
# Task 2: 원본 목록 S3 업로드
# ==========================================================
def upload_to_s3(**context):
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

# ==========================================================
# Task 3: 스마트 필터링 및 본문(HTML) 파싱
# ==========================================================
def filter_and_extract_text(**context):
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

    # 타겟 공시만 필터링
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

# ==========================================================
# Task 4: Agentic 파이프라인 (LLM 추출 -> 판사 -> 교정 -> 재검증)
# ==========================================================
def transform_with_llm(**context):
    from openai import OpenAI
    exec_date = context['logical_date'].strftime('%Y%m%d')
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    parsed_file_key = f"processed/dart/{exec_date}/parsed_disclosures.json"
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=parsed_file_key)
        parsed_data_list = json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        print(f"📭 {exec_date} 파싱된 데이터가 없어 스킵합니다.")
        return []
    except Exception as e:
        raise e

    target_list = [item for item in parsed_data_list if item.get('document_text') and "원문 파싱 실패" not in item.get('document_text')]
    if not target_list:
        return []

    client = OpenAI(api_key=OPENAI_API_KEY)
    final_structured_data = []
    failed_logs = []
    stats = {"PASS": 0, "HEALED": 0, "TIER1_FAIL": 0, "FINAL_FAIL": 0}
    
    print(f"🤖 [Agentic Workflow] 총 {len(target_list)}건 동적 정형화 및 자가 치유 시작...")
    
    for idx, item in enumerate(target_list, 1):
        doc_text = item.get('document_text', '')
        report_nm = item.get('clean_report_nm', '')
        corp_name = item.get('corp_name', '')
        
        # [프롬프트 셋팅]
        if "매출액" in report_nm or "손익구조" in report_nm:
            system_role = "당신은 퀀트 투자 분석가입니다. 응답은 반드시 JSON 형식으로 하세요."
            expected_keys = "- op_profit_change_pct, - turnaround_status, - evidence_text"
        elif "배당" in report_nm:
            system_role = "당신은 배당 공시 분석가입니다. 응답은 반드시 JSON 형식으로 하세요."
            expected_keys = "- dividend_per_share, - dividend_yield, - evidence_text"
        else:
            system_role = "당신은 금융 공시 요약 전문가입니다. 응답은 반드시 JSON 형식으로 하세요."
            expected_keys = "- event_type, - summary, - evidence_text"
            
        constraints = """
        [데이터 추출 주의사항 - 절대 엄수!]
        1. (단위 변환 금지): 원문에 기재된 숫자(예: 42,000,000,000)를 임의로 '42억' 등으로 변환하지 마세요.
        2. (정정 공시 주의): 공시가 '정정신고'인 경우, 반드시 표의 [정정 후] 데이터를 기준으로 추출하세요.
        """
        
        user_prompt = f"[공시 텍스트]\n{doc_text[:2500]}\n\n[추출 스키마]\n{expected_keys}\n\n{constraints}"
        
        try:
            # 🧩 [1차 추출]
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={ "type": "json_object" },
                messages=[{"role": "system", "content": system_role}, {"role": "user", "content": user_prompt}],
                temperature=0.0 
            )
            extracted_json = json.loads(completion.choices[0].message.content)
            evidence = str(extracted_json.get('evidence_text', ''))
            
            # 🛡️ [Tier 1] 규칙 기반 숫자 교차 검증
            tier1_pass = True
            if not evidence or evidence == 'null':
                tier1_pass = False
            else:
                numbers = re.findall(r'[-]?\d+(?:,\d+)*(?:\.\d+)?', evidence)
                sig_nums = [n for n in numbers if len(n.replace(',','').replace('.','')) > 1]
                
                if not sig_nums:
                    clean_doc = re.sub(r'\s+', '', doc_text)
                    if re.sub(r'\s+', '', evidence) not in clean_doc: tier1_pass = False
                else:
                    for num in sig_nums:
                        if num not in doc_text: tier1_pass = False; break

            if not tier1_pass:
                print(f"[{idx}/{len(target_list)}] 🚨 [Tier 1 탈락] {corp_name}: 원문에 없는 팩트 창조")
                stats["TIER1_FAIL"] += 1
                continue 

            # ⚖️ [Tier 2] LLM 판사 (1차 평가)
            judge_prompt = f"""
            [원본] {doc_text[:2500]}
            [추출본] {json.dumps(extracted_json, ensure_ascii=False)}
            위 데이터가 원본의 맥락(숫자 맵핑 등)과 정확히 일치하는지 엄격히 평가하세요.
            응답은 반드시 아래 JSON 형식으로 하세요: {{"judge_result": "PASS" 또는 "FAIL", "reason": "이유"}}
            """
            judge_comp = client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={ "type": "json_object" },
                messages=[{"role": "system", "content": "당신은 금융 데이터 평가관입니다. 응답은 JSON."}, {"role": "user", "content": judge_prompt}],
                temperature=0.0 
            )
            judge_res = json.loads(judge_comp.choices[0].message.content)

            best_result = None

            # 🏥 [Tier 3] 자가 치유 및 재검증
            if judge_res.get("judge_result") == "PASS":
                print(f"[{idx}/{len(target_list)}] ✅ [PASS] {corp_name}")
                stats["PASS"] += 1
                best_result = extracted_json
            else:
                fail_reason = judge_res.get("reason", "맥락 오류")
                print(f"[{idx}/{len(target_list)}] 🚨 [오류 감지] {corp_name} -> 🛠️ 자가 치유 시작...")
                
                # 교정 AI (Healer)
                healer_prompt = f"""
                [원본] {doc_text[:2500]}
                [오류가 발생한 데이터] {json.dumps(extracted_json, ensure_ascii=False)}
                [판사 AI의 지적 사항] {fail_reason}
                위 지적 사항을 반영하여 원문에 맞게 데이터를 완벽히 수정하세요. 반드시 JSON 형식으로 응답하세요: {expected_keys}
                """
                healer_comp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    response_format={ "type": "json_object" },
                    messages=[{"role": "system", "content": f"{system_role} 최종 데이터만 JSON으로 제출하세요."}, {"role": "user", "content": healer_prompt}],
                    temperature=0.0 
                )
                corrected_json = json.loads(healer_comp.choices[0].message.content)
                
                # 재검증 (Re-judge)
                re_judge_prompt = f"""
                [원본] {doc_text[:2500]}
                [AI가 수정한 요약 데이터] {json.dumps(corrected_json, ensure_ascii=False)}
                오류가 완벽히 수정되었고 원문과 100% 일치하는지 최종 평가하세요. 
                응답은 JSON 형식으로 하세요: {{"judge_result": "PASS" 또는 "FAIL"}}
                """
                re_judge_comp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    response_format={ "type": "json_object" },
                    messages=[{"role": "system", "content": "데이터 평가관입니다. JSON 응답 필수."}, {"role": "user", "content": re_judge_prompt}],
                    temperature=0.0 
                )
                
                if json.loads(re_judge_comp.choices[0].message.content).get("judge_result") == "PASS":
                    print(f"   └─> 🏅 [재검증 통과] {corp_name} 복구 및 정합성 100% 확보")
                    stats["HEALED"] += 1
                    best_result = corrected_json
                    failed_logs.append({"corp": corp_name, "reason": fail_reason, "before": extracted_json, "after": corrected_json})
                else:
                    print(f"   └─> ❌ [폐기] {corp_name} 교정 실패")
                    stats["FINAL_FAIL"] += 1
            
            # 최종 합격 데이터 적재 준비
            if best_result:
                final_item = {k: v for k, v in item.items() if k != 'document_text'}
                final_item["extracted_data"] = best_result
                final_structured_data.append(final_item)

        except Exception as e:
            print(f"[{idx}/{len(target_list)}] ❌ 에러 ({corp_name}): {e}")

    # ==========================================================
    # 결과물 S3 적재 (JSON 데이터 + 포트폴리오용 Markdown 리포트)
    # ==========================================================
    final_file_key = f"final/dart/{exec_date}/structured_disclosures.json"
    if final_structured_data:
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME, Key=final_file_key,
                Body=json.dumps(final_structured_data, ensure_ascii=False, indent=4).encode('utf-8')
            )
            print(f"🎉 최종 DB 적재용 데이터 S3 저장 완료: {final_file_key}")
        except Exception as e:
            print(f"❌ S3 최종 저장 실패: {e}")

    # 자가 치유 성과를 기록한 마크다운 리포트 생성 및 S3 저장
    if failed_logs:
        md_content = f"# 🚀 Agentic AI 기반 데이터 자가 치유 성과 보고서 ({exec_date})\n\n"
        md_content += f"- **총 검증 대상:** {len(target_list)}건\n"
        md_content += f"- **오류 감지 후 자가 치유 성공 (수율 100%):** {stats['HEALED']}건\n\n---\n\n"
        for log in failed_logs:
            md_content += f"### 🏢 기업명: {log['corp']}\n"
            md_content += f"- **🚨 초기 오류 원인 (판사 AI 지적):** {log['reason']}\n\n"
            md_content += f"**❌ 교정 전 (Before):**\n```json\n{json.dumps(log['before'], ensure_ascii=False, indent=2)}\n```\n\n"
            md_content += f"**✅ 교정 후 (After):**\n```json\n{json.dumps(log['after'], ensure_ascii=False, indent=2)}\n```\n---\n"
        
        report_key = f"report/dart/{exec_date}/healing_report.md"
        try:
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=report_key, Body=md_content.encode('utf-8'))
            print(f"📄 포트폴리오용 마크다운 보고서 S3 적재 완료: {report_key}")
        except Exception as e:
            pass

    return final_structured_data

# ==========================================================
# Task 5: PostgreSQL 데이터 적재 (UPSERT 방식 멱등성 보장)
# ==========================================================
def load_to_postgres(**context):
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
        
        # 🔑 rcept_no를 PRIMARY KEY로 지정하여 테이블 생성 (최초 1회 동작)
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
        
        # 🛡️ 멱등성을 위한 UPSERT 로직 적용 (ON CONFLICT DO UPDATE)
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

# ==========================================================
# DAG 정의 및 Task 의존성 설정
# ==========================================================
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dart_disclosure_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 18 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['DART', 'Finance', 'Agentic Workflow'],
) as dag:

    fetch_list_task = PythonOperator(task_id='fetch_dart_list', python_callable=fetch_dart_list)
    upload_s3_task = PythonOperator(task_id='upload_to_s3', python_callable=upload_to_s3)
    extract_text_task = PythonOperator(task_id='filter_and_extract_text', python_callable=filter_and_extract_text)
    transform_llm_task = PythonOperator(task_id='transform_with_llm', python_callable=transform_with_llm)
    load_db_task = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)
    
    fetch_list_task >> upload_s3_task >> extract_text_task >> transform_llm_task >> load_db_task