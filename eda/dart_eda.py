import json
import os
from collections import Counter

# 1. 로컬에 있는 JSON 파일 경로를 지정해주세요.
# 스크립트와 같은 폴더에 있다면 파일명만 적으시면 됩니다.
LOCAL_FILE_PATH = "D:\DisclosureInsight\eda\parsed_disclosures.json"

def run_eda():
    print(f"📥 로컬 파일에서 데이터 불러오는 중: {LOCAL_FILE_PATH}")
    
    # 파일 존재 여부 확인
    if not os.path.exists(LOCAL_FILE_PATH):
        print(f"❌ 파일을 찾을 수 없습니다. 경로를 확인해주세요: {LOCAL_FILE_PATH}")
        return

    # 파일 읽기
    try:
        with open(LOCAL_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ 파일 읽기/파싱 실패: {e}")
        return

    print(f"\n📊 총 필터링된 공시 건수: {len(data)}건")

    # 원문 파싱에 성공한 데이터만 추려내기
    valid_data = [item for item in data if not item.get('document_text', '').startswith('원문 파싱 실패')]
    print(f"✅ 원문 파싱 성공 건수: {len(valid_data)}건")

    # 공시 유형별(clean_report_nm) 빈도수 분석
    report_types = [item.get('clean_report_nm', '알수없음') for item in valid_data]
    type_counts = Counter(report_types)

    print("\n🏆 [공시 유형별 빈도수 TOP 5]")
    for r_type, count in type_counts.most_common(5):
        print(f" - {r_type}: {count}건")

    # 상위 3개 유형의 원문 텍스트 샘플 까보기 (LLM 프롬프트 기획용)
    print("\n🔎 [상위 3개 공시 유형 원문 샘플 (앞 600자)]")
    for r_type, _ in type_counts.most_common(3):
        print(f"\n{'='*60}")
        print(f"📌 공시 유형: {r_type}")
        print(f"{'='*60}")
        
        # 해당 유형의 첫 번째 샘플 텍스트 출력
        for item in valid_data:
            if item.get('clean_report_nm') == r_type:
                print(f"🏢 기업명: {item.get('corp_name')}")
                print(f"📝 원문 텍스트:\n{item.get('document_text')[:600]} ... (이하 생략)")
                break

if __name__ == "__main__":
    run_eda()