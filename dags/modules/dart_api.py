# dags/modules/dart_api.py
import os
import requests

def fetch_dart_list(**context):
    DART_API_KEY = os.getenv("DART_API_KEY")
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