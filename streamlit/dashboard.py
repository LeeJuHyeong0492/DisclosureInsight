import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import psycopg2
import json
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import numpy as np

# ==========================================
# 1. 페이지 세팅
# ==========================================
st.set_page_config(page_title="DART & 주가 인사이트", page_icon="📈", layout="wide")
st.title("📈 퀀트 인사이트 프로 (Klinecharts 에디션)")
st.markdown("트레이딩뷰 수준의 부드러운 차트와 공시 이벤트를 연동한 전문가용 대시보드입니다.")

# ==========================================
# 2. DB 데이터 로드 및 전처리
# ==========================================
@st.cache_data(ttl=600)
def load_data():
    try:
        conn = psycopg2.connect(
            host="postgres-dw", port=5432, dbname="finance_dw", user="dw_user", password="dw_password"
        )
        df = pd.read_sql("SELECT * FROM dart_disclosures;", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return pd.DataFrame()

df = load_data()
if df.empty:
    st.warning("데이터가 없습니다. 파이프라인을 실행해주세요.")
    st.stop()

# JSON 파싱 및 날짜 전처리
extracted_df = df['extracted_data'].apply(
    lambda x: x if isinstance(x, dict) else (json.loads(x) if x else {})
).apply(pd.Series)
final_df = pd.concat([df, extracted_df], axis=1)
final_df['date'] = pd.to_datetime(final_df['rcept_dt'], format='%Y%m%d')
final_df['stock_code'] = final_df['stock_code'].astype(str).str.zfill(6)

# ==========================================
# 3. 탭(Tabs) UI 구성
# ==========================================
tab1, tab2, tab3 = st.tabs(["📊 Kline 차트 분석", "🔍 조건 스크리너", "🧪 이벤트 백테스트"])

# ------------------------------------------
# [Tab 1] Klinecharts (마커 및 툴팁 최적화)
# ------------------------------------------
with tab1:
    st.sidebar.header("🔍 [차트] 분석 기업 선택")
    corp_list = final_df['corp_name'].unique().tolist()
    selected_corp = st.sidebar.selectbox("기업을 선택하세요", corp_list)

    corp_df = final_df[final_df['corp_name'] == selected_corp].copy()
    stock_code = corp_df['stock_code'].iloc[0]

    min_date = corp_df['date'].min() - timedelta(days=90)
    max_date = datetime.today()

    with st.spinner(f"📡 {selected_corp} 주가 데이터 불러오는 중..."):
        try:
            price_df = fdr.DataReader(stock_code, min_date, max_date).reset_index()
        except Exception:
            st.error("주가 데이터를 불러오는데 실패했습니다.")
            st.stop()

    # --- 💡 [수정됨] 차트 데이터 및 마커(점) 데이터 생성 ---
    kline_data = []
    annotations = []

    for _, row in price_df.iterrows():
        timestamp = int(row['Date'].timestamp() * 1000)
        
        # 기본 캔들 데이터
        kline = {
            "timestamp": timestamp,
            "open": row['Open'],
            "high": row['High'],
            "low": row['Low'],
            "close": row['Close'],
            "volume": row['Volume']
        }
        
        # 해당 날짜에 공시가 있는지 확인
        match_corp = corp_df[corp_df['date'] == row['Date']]
        if not match_corp.empty:
            evt_row = match_corp.iloc[0] # 여러 개면 첫 번째 공시 기준
            
            # 호재/악재 색상 판별
            impact_color = '#888888' # 중립(회색)
            if pd.notna(evt_row.get('op_profit_change_pct')):
                impact_color = '#ef5350' if float(evt_row['op_profit_change_pct']) > 0 else '#26a69a'
            elif any(kw in str(evt_row.get('clean_report_nm', '')) for kw in ['수주', '공급계약', '흑자전환', '무상증자']):
                impact_color = '#ef5350' # 호재(빨강)
            elif any(kw in str(evt_row.get('clean_report_nm', '')) for kw in ['유상증자', '적자전환', '소송', '횡령', '배임']):
                impact_color = '#26a69a' # 악재(파랑)

            title = str(evt_row.get('clean_report_nm', ''))
            
            # 1. 캔들 객체에 툴팁용 데이터 숨겨두기
            kline['eventTitle'] = title
            kline['eventColor'] = impact_color
            
            # 2. 캔들 밑에 찍을 '심플한 점(Dot)' 데이터 만들기 (Y축은 저가(Low) 적용)
            annotations.append({
                "timestamp": timestamp,
                "value": row['Low'], # 👈 캔들의 꼬리 아래에 점을 찍기 위한 정확한 Y좌표!
                "color": impact_color
            })
            
        kline_data.append(kline)

    # --- 💡 [수정됨] HTML/JS 렌더링 코드 ---
    html_code = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="utf-8" />
        <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/klinecharts/dist/klinecharts.min.js"></script>
        <style>
            body {{ margin: 0; padding: 0; background-color: #ffffff; }}
            #kline-chart {{ width: 100%; height: 600px; }}
        </style>
    </head>
    <body>
        <div id="kline-chart"></div>
        <script>
            var chart = klinecharts.init('kline-chart');
            
            // 트레이딩뷰 스타일 및 마우스 호버(Tooltip) 설정
            chart.setStyles({{
                candle: {{
                    bar: {{
                        upColor: '#ef5350', downColor: '#26a69a',
                        noChangeColor: '#888888', upBorderColor: '#ef5350',
                        downBorderColor: '#26a69a', noChangeBorderColor: '#888888',
                        upWickColor: '#ef5350', downWickColor: '#26a69a',
                        noChangeWickColor: '#888888'
                    }},
                    tooltip: {{
                        // 마우스를 올렸을 때 공시 정보가 있으면 크로스헤어에 추가 표시
                        custom: function(data) {{
                            var kline = data.current;
                            if (kline && kline.eventTitle) {{
                                return [
                                    {{ title: '🔔 이벤트', value: kline.eventTitle, color: kline.eventColor }}
                                ];
                            }}
                            return [];
                        }}
                    }}
                }}
            }});

            var chartData = {json.dumps(kline_data)};
            chart.applyNewData(chartData);
            chart.createIndicator('VOL', false, {{ height: 100 }});

            // 말풍선을 없애고 심플한 '점(Dot)'으로만 표시
            var annotations = {json.dumps(annotations)};
            annotations.forEach(function(anno) {{
                chart.createOverlay({{
                    name: 'simpleAnnotation',
                    extendData: '', // 텍스트 완전히 제거
                    points: [{{ timestamp: anno.timestamp, value: anno.value }}],
                    styles: {{
                        point: {{
                            show: true,
                            backgroundColor: anno.color,
                            borderColor: '#ffffff',
                            borderSize: 1,
                            radius: 4 // 차트를 가리지 않는 작고 깔끔한 사이즈
                        }},
                        line: {{ show: false }}, // 선 숨김
                        text: {{ show: false }}  // 말풍선 숨김
                    }}
                }});
            }});
        </script>
    </body>
    </html>
    """
    components.html(html_code, height=620)
    
# ------------------------------------------
# [Tab 2] 퀀트 조건 스크리너 (기존과 동일)
# ------------------------------------------
with tab2:
    st.subheader("🔎 LLM 정형 데이터 기반 조건 검색")
    col1, col2 = st.columns(2)
    with col1:
        if 'op_profit_change_pct' in final_df.columns:
            min_profit = st.slider("최소 영업이익 증감률 (%)", -100, 200, 10)
    with col2:
        if 'event_type' in final_df.columns:
            available_events = final_df['event_type'].dropna().unique().tolist()
            selected_events = st.multiselect("특정 이벤트 포함", available_events)

    screened_df = final_df.copy()
    if 'op_profit_change_pct' in final_df.columns:
        screened_df['op_profit_change_pct'] = pd.to_numeric(screened_df['op_profit_change_pct'], errors='coerce')
        screened_df = screened_df[(screened_df['op_profit_change_pct'].isna()) | (screened_df['op_profit_change_pct'] >= min_profit)]
    if 'event_type' in final_df.columns and selected_events:
        screened_df = screened_df[screened_df['event_type'].isin(selected_events)]

    st.success(f"총 {len(screened_df)}건의 공시가 검색되었습니다.")
    st.dataframe(screened_df[['corp_name', 'rcept_dt', 'clean_report_nm', 'event_type', 'op_profit_change_pct', 'summary']], use_container_width=True)

# ------------------------------------------
# [Tab 3] 이벤트 백테스트 통계 (기존과 동일)
# ------------------------------------------
with tab3:
    st.subheader("🧪 공시 발생 후 주가 수익률 분석 (T+N 백테스트)")
    if st.button(f"{selected_corp} 이벤트 수익률 계산하기", type="primary"):
        results = []
        with st.spinner("주가 변동 계산 중..."):
            for _, row in corp_df.iterrows():
                event_date = row['date']
                try:
                    t_0_idx = price_df[price_df['Date'] >= event_date].index[0]
                    t_0_price = price_df.loc[t_0_idx, 'Close']
                    t_3_price = price_df.loc[t_0_idx + 3, 'Close'] if (t_0_idx + 3) < len(price_df) else None
                    t_5_price = price_df.loc[t_0_idx + 5, 'Close'] if (t_0_idx + 5) < len(price_df) else None
                    
                    ret_3 = round(((t_3_price - t_0_price) / t_0_price) * 100, 2) if t_3_price else "데이터 부족"
                    ret_5 = round(((t_5_price - t_0_price) / t_0_price) * 100, 2) if t_5_price else "데이터 부족"
                    
                    results.append({
                        "공시일": event_date.strftime('%Y-%m-%d'), "공시 제목": row['clean_report_nm'],
                        "당일 종가": f"{t_0_price:,}원", "T+3 수익률": f"{ret_3}%" if isinstance(ret_3, float) else ret_3, "T+5 수익률": f"{ret_5}%" if isinstance(ret_5, float) else ret_5
                    })
                except Exception:
                    pass
        if results:
            st.dataframe(pd.DataFrame(results), use_container_width=True)
        else:
            st.warning("분석할 수 있는 주가 데이터가 부족합니다.")