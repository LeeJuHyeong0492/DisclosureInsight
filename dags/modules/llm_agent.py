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
    stats = {"PASS": 0, "HEALED": 0, "FINAL_FAIL": 0}
    
    print(f"🤖 [Agentic Workflow] 총 {len(target_list)}건 동적 정형화 및 'Reflexion(반성)' 피드백 루프 시작...")
    
    for idx, item in enumerate(target_list, 1):
        doc_text = item.get('document_text', '')
        report_nm = item.get('clean_report_nm', '')
        corp_name = item.get('corp_name', '')
        
        # [프롬프트 셋팅]
        if "매출액" in report_nm or "손익구조" in report_nm:
            system_role = "당신은 퀀트 투자 분석가입니다. 응답은 반드시 JSON 형식으로 하세요."
            expected_keys = "- op_profit_change_pct (오직 '영업이익'의 증감비율만. 단일 숫자(float)로 표기, 절대 JSON 하위 객체를 만들지 말 것), - turnaround_status (흑자전환, 적자전환, 적자지속, 흑자지속 중 택1), - evidence_text"
        elif "배당" in report_nm:
            system_role = "당신은 배당 공시 분석가입니다. 응답은 반드시 JSON 형식으로 하세요."
            expected_keys = "- dividend_per_share (숫자만), - dividend_yield (숫자만), - evidence_text"
        else:
            system_role = "당신은 금융 공시 요약 전문가입니다. 응답은 반드시 JSON 형식으로 하세요."
            expected_keys = "- event_type, - summary, - evidence_text"
            
        constraints = """
        [데이터 추출 엄격 주의사항!]
        1. 포맷 규칙: '%'나 '원' 같은 기호는 빼고 순수 숫자만 적으세요. (예: -132.0% -> -132.0)
        2. 없는 정보 창조 금지: 원문에 해당 지표가 명시되어 있지 않다면, 절대 억지로 지어내지 말고 반드시 `null` 을 입력하세요.
        3. 정정공시는 반드시 [정정 후] 데이터를 기준으로 하세요.
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
            
            # ⚖️ [Tier 2] LLM 판사 (1차 평가 - 💡 융통성 탑재 및 사칙연산 금지!)
            judge_prompt = f"""
            [원본] {doc_text[:2500]}
            [추출본] {json.dumps(extracted_json, ensure_ascii=False)}
            
            당신은 매우 기계적이고 엄격한 팩트체커입니다. 추출본의 데이터가 원본의 텍스트에 물리적으로 존재하는지만 평가하세요.
            [합격(PASS) 기준 - 매우 중요]
            1. 기호 생략은 PASS: 원문에 '-132.0%'로 표기되었으나 추출본에 '-132.0' (숫자만)으로 기재된 것은 완벽한 정답입니다.
            2. 빈 값 처리는 PASS: 원문에 항목이 아예 없거나 '-'로 표기되어 추출본에 null, "N/A", "해당없음", "" 등으로 표기된 것은 정상입니다.
            3. 요약은 PASS: evidence_text는 핵심 요약이어도 팩트가 맞으면 합격입니다.
            4. ★사칙연산 절대 금지★: 절대 직접 계산하지 마세요.
            5. ★정정공시 절대 원칙★: 원문이 [기재정정] 공시이거나 표에 "정정 전", "정정 후"가 모두 있을 경우, 무조건 "정정 후"의 숫자만 진짜 팩트로 인정하세요. "정정 전" 숫자와 다르다고 지적하면 절대 안 됩니다.
            
            오직 '추출본의 숫자가 원문의 (정정 후) 숫자와 아예 다를 경우'나 '원문에 없는 내용을 지어낸 경우'에만 FAIL을 주고 correction_guideline을 작성하세요.
            
            응답은 반드시 아래 JSON 형식으로 하세요: 
            {{
                "judge_result": "PASS" 또는 "FAIL", 
                "reason": "오류 사유 (PASS면 비워둠)",
                "correction_guideline": "구체적인 수정 지시사항 (PASS면 비워둠)"
            }}
            """
            judge_comp = client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={ "type": "json_object" },
                messages=[{"role": "system", "content": "당신은 합리적이고 팩트 중심적인 금융 데이터 평가관입니다. 응답은 JSON."}, {"role": "user", "content": judge_prompt}],
                temperature=0.0 
            )
            judge_res = json.loads(judge_comp.choices[0].message.content)

            best_result = None

            # 🏥 [Tier 3] 다중 턴 자가 치유 (최대 2회 반복)
            if judge_res.get("judge_result") == "PASS":
                print(f"[{idx}/{len(target_list)}] ✅ [PASS] {corp_name}")
                stats["PASS"] += 1
                best_result = extracted_json
            else:
                fail_reason = judge_res.get("reason", "팩트 오류")
                correction_guideline = judge_res.get("correction_guideline", "수정 지시사항 없음")
                
                print(f"[{idx}/{len(target_list)}] 🚨 [오류 감지] {corp_name}")
                print(f"   ├─> 🧑‍⚖️ 판사 지적: {fail_reason}")
                print(f"   └─> 💡 수정 가이드: {correction_guideline}")
                print(f"   └─> 🛠️ 자가 치유 루프 시작...")
                
                current_json = extracted_json
                healed_success = False
                
                for attempt in range(1, 3):
                    healer_prompt = f"""
                    [원본] {doc_text[:2500]}
                    [오류가 발생한 데이터] {json.dumps(current_json, ensure_ascii=False)}
                    [평가관의 지적 사항] {fail_reason}
                    [★평가관의 구체적 수정 가이드라인★] {correction_guideline}
                    
                    당신은 오류를 수정하는 전문가입니다. 평가관의 '수정 가이드라인'을 완벽하게 준수하여 데이터를 고치세요.
                    주의: 원문에 해당 정보가 아예 없다면 억지로 만들지 말고 반드시 `null` 로 입력하세요.
                    반드시 아래 스키마에 맞춰 JSON 형식으로 응답하세요: 
                    {expected_keys}
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
                    [이전 지적 사항 및 가이드라인] {correction_guideline}
                    [AI가 수정한 데이터] {json.dumps(corrected_json, ensure_ascii=False)}
                    
                    수정된 데이터가 이전 가이드라인을 잘 수행했는지 팩트체크 하세요.
                    [평가 절대 원칙]
                    1. 단위 기호 생략, 빈 값의 null 처리는 완벽한 정답입니다.
                    2. 절대 직접 계산을 하지 마세요. 원문 텍스트에 있는 숫자와 텍스트 매칭만 하세요.
                    3. 정정공시인 경우 반드시 "정정 후"의 데이터와 일치하는지 확인하세요.
                    4. "A로 수정되어야 하는데 A로 잘못 기재되었다"는 식의 모순된 평가는 절대 하지 마세요. 추출본의 값이 당신이 요구한 정답 값과 같다면 무조건 PASS를 주어야 합니다.
                    
                    응답은 JSON 형식으로 하세요: 
                    {{
                        "judge_result": "PASS" 또는 "FAIL", 
                        "reason": "실패시 상세 사유",
                        "correction_guideline": "다시 실패했을 경우를 위한 추가 수정 지시사항"
                    }}
                    """
                    re_judge_comp = client.chat.completions.create(
                        model="gpt-4o-mini",
                        response_format={ "type": "json_object" },
                        messages=[{"role": "system", "content": "데이터 평가관입니다. JSON 응답 필수."}, {"role": "user", "content": re_judge_prompt}],
                        temperature=0.0 
                    )
                    re_judge_res = json.loads(re_judge_comp.choices[0].message.content)
                    
                    if re_judge_res.get("judge_result") == "PASS":
                        print(f"   └─> 🏅 [{attempt}차 재검증 통과] {corp_name} 복구 성공!")
                        stats["HEALED"] += 1
                        best_result = corrected_json
                        failed_logs.append({"corp": corp_name, "reason": fail_reason, "guideline": correction_guideline, "before": extracted_json, "after": corrected_json})
                        healed_success = True
                        break 
                    else:
                        print(f"   └─> ⚠️ [{attempt}차 실패] 사유: {re_judge_res.get('reason')}")
                        current_json = corrected_json
                        fail_reason = re_judge_res.get("reason")
                        correction_guideline = re_judge_res.get("correction_guideline", "추가 지시사항 없음")
                
                if not healed_success:
                    print(f"   └─> ❌ [최종 검증 실패] {corp_name} (최대 교정 횟수 초과)")
                    stats["FINAL_FAIL"] += 1
            
            # [Soft Fail] 최종 합격 여부와 상관없이 무조건 DB 적재용 리스트에 담기
            final_item = {k: v for k, v in item.items() if k != 'document_text'}
            if best_result:
                final_item["extracted_data"] = best_result
            else:
                final_item["extracted_data"] = {"error": "AI 요약 및 검증 실패 (복잡한 공시이므로 원문 확인 요망)"}
            
            final_structured_data.append(final_item)

        except Exception as e:
            print(f"[{idx}/{len(target_list)}] ❌ 에러 ({corp_name}): {e}")

    # ==========================================================
    # 결과물 S3 적재
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

    if failed_logs:
        md_content = f"# 🚀 Agentic AI 기반 데이터 자가 치유 성과 보고서 ({exec_date})\n\n"
        md_content += f"- **총 검증 대상:** {len(target_list)}건\n"
        md_content += f"- **초기 통과(PASS):** {stats['PASS']}건\n"
        md_content += f"- **오류 감지 후 자가 치유 성공(HEALED):** {stats['HEALED']}건\n"
        md_content += f"- **최종 폐기(Soft Fail 처리):** {stats['FINAL_FAIL']}건\n\n---\n\n"
        for log in failed_logs:
            md_content += f"### 🏢 기업명: {log['corp']}\n"
            md_content += f"- **🚨 초기 오류 원인:** {log['reason']}\n"
            md_content += f"- **💡 판사 AI 수정 가이드:** {log.get('guideline', '')}\n\n"
            md_content += f"**❌ 교정 전 (Before):**\n```json\n{json.dumps(log['before'], ensure_ascii=False, indent=2)}\n```\n\n"
            md_content += f"**✅ 교정 후 (After):**\n```json\n{json.dumps(log['after'], ensure_ascii=False, indent=2)}\n```\n---\n"
        
        report_key = f"report/dart/{exec_date}/healing_report.md"
        try:
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=report_key, Body=md_content.encode('utf-8'))
        except Exception:
            pass

    return final_structured_data