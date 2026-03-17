import json
import re
from collections import Counter

def analyze_raw_dart_data():
    # 1. 원본 JSON 파일 읽기
    try:
        with open('D:\DisclosureInsight\eda\disclosure_list.json', 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        print("❌ 'disclosure_list.json' 파일을 찾을 수 없습니다.")
        return

    print(f"📊 [Raw Data 탐색적 분석(EDA) 시작] 총 데이터 수: {len(raw_data)}건\n")

    # 2. 시장 구분(corp_cls) 분포 분석
    # 데이터가 코스피, 코스닥, 펀드 등 어디에 얼마나 몰려있는지 확인합니다.
    corp_cls_list = [item.get('corp_cls', '알수없음') for item in raw_data]
    corp_cls_counts = Counter(corp_cls_list)
    
    print("🏢 1. 시장 구분(corp_cls) 데이터 분포:")
    for cls, count in corp_cls_counts.most_common():
        if cls == 'Y': desc = "유가증권(KOSPI)"
        elif cls == 'K': desc = "코스닥(KOSDAQ)"
        elif cls == 'E': desc = "기타법인/펀드/ETF"
        elif cls == 'N': desc = "코넥스"
        else: desc = "기타"
        print(f"  - '{cls}' ({desc}): {count}건")

    # 3. 공시 제목(report_nm) 패턴 빈도 분석
    # 제목에 붙은 [기재정정], (자율공시) 같은 껍데기만 살짝 벗겨내고, 
    # 어떤 종류의 공시가 가장 많이 올라왔는지 순위를 매겨봅니다.
    cleaned_titles = []
    for item in raw_data:
        title = item.get('report_nm', '')
        # 괄호 안의 내용만 제거하여 순수 이벤트 명칭만 추출
        cleaned = re.sub(r'\[.*?\]|\(.*?\)', '', title).strip()
        if cleaned:
            cleaned_titles.append(cleaned)

    title_counts = Counter(cleaned_titles)

    print("\n📝 2. 가장 많이 발생한 공시 제목 TOP 20 (태그 제거 후 원본 기준):")
    for title, count in title_counts.most_common(20):
        print(f"  - {title} : {count}건")

    print("\n=================================================")
    print("💡 이 통계 결과를 보고, 우리가 버릴 데이터와 취할 데이터를 직접 결정해 봅시다!")

if __name__ == "__main__":
    analyze_raw_dart_data()