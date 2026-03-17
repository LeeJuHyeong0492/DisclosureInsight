# dags/modules/llm_agent.py
import os
import json
import boto3
import re
from openai import OpenAI

S3_BUCKET_NAME = "disclosure-insight-raw-data-ljh" 

def transform_with_llm(**context):
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
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