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
st.title("📈 퀀트 인사이트")
st.markdown("차트와 공시 이벤트를 연동한 대시보드입니다.")

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
tab1, tab2, tab3 = st.tabs(["📊 차트 분석", "🔍 조건 스크리너", "🧪 이벤트 백테스트"])

# ------------------------------------------
# [Tab 1] Klinecharts (마커 및 툴팁 최적화 + 상세 정보 패널)
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

    # --- 💡 1. 차트 데이터 및 풍부한 상세 정보(eventDetails) 셋팅 ---
    kline_data = []
    annotations = []

    for _, row in price_df.iterrows():
        timestamp = int(row['Date'].timestamp() * 1000)
        
        kline = {
            "timestamp": timestamp,
            "open": row['Open'],
            "high": row['High'],
            "low": row['Low'],
            "close": row['Close'],
            "volume": row['Volume']
        }
        
        match_corp = corp_df[corp_df['date'] == row['Date']]
        if not match_corp.empty:
            evt_row = match_corp.iloc[0] 
            
            # 호재/악재 색상 판별
            impact_color = '#888888'
            if pd.notna(evt_row.get('op_profit_change_pct')):
                impact_color = '#ef5350' if float(evt_row['op_profit_change_pct']) > 0 else '#26a69a'
            elif any(kw in str(evt_row.get('clean_report_nm', '')) for kw in ['수주', '공급계약', '흑자전환', '무상증자']):
                impact_color = '#ef5350'
            elif any(kw in str(evt_row.get('clean_report_nm', '')) for kw in ['유상증자', '적자전환', '소송', '횡령', '배임']):
                impact_color = '#26a69a'

            title = str(evt_row.get('clean_report_nm', ''))
            
            # LLM이 추출한 상세 데이터 조합하기
            extra_info = []
            if pd.notna(evt_row.get('op_profit_change_pct')):
                extra_info.append(f"영업이익 증감률: {evt_row['op_profit_change_pct']}%")
            if pd.notna(evt_row.get('dividend_yield')):
                extra_info.append(f"시가배당률: {evt_row['dividend_yield']}%")
            if pd.notna(evt_row.get('turnaround_status')):
                extra_info.append(f"상태: {evt_row['turnaround_status']}")
            
            summary_text = str(evt_row.get('summary', '상세 요약 내용이 없습니다.'))
            if summary_text == 'nan': summary_text = '상세 요약 내용이 없습니다.'

            # 자바스크립트로 넘길 상세 객체 생성
            kline['eventDetails'] = {
                "title": title,
                "date": row['Date'].strftime('%Y-%m-%d'),
                "color": impact_color,
                "summary": summary_text,
                "extra": " | ".join(extra_info) if extra_info else "추가 수치 데이터 없음"
            }
            
            # 캔들을 가리지 않도록 위치를 조정(offset)한 심플 마커
            annotations.append({
                "timestamp": timestamp,
                "value": row['Low'], 
                "color": impact_color
            })
            
        kline_data.append(kline)

    # --- 💡 2. 분리형 UI (차트 + 하단 정보 패널) HTML/JS 렌더링 ---
    html_code = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="utf-8" />
        <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/klinecharts/dist/klinecharts.min.js"></script>
        <style>
            body {{ margin: 0; padding: 0; background-color: #ffffff; display: flex; flex-direction: column; height: 100vh; font-family: 'Malgun Gothic', sans-serif; }}
            #kline-chart {{ flex: 1; min-height: 550px; }}
            #event-panel {{
                height: 120px; 
                background-color: #f8f9fa; 
                border-top: 2px solid #e9ecef; 
                padding: 15px 25px; 
                box-sizing: border-box;
                overflow-y: auto;
            }}
            .empty-text {{ color: #adb5bd; text-align: center; margin-top: 25px; font-size: 15px; font-weight: bold; }}
            .evt-title {{ margin: 0 0 8px 0; font-size: 16px; font-weight: 800; }}
            .evt-summary {{ margin: 0 0 5px 0; font-size: 14px; color: #495057; line-height: 1.4; }}
            .evt-extra {{ margin: 0; font-size: 13px; font-weight: 700; color: #0d6efd; }}
        </style>
    </head>
    <body>
        <div id="kline-chart"></div>
        <div id="event-panel">
            <div class="empty-text">🖱️ 차트의 마커(점)에 마우스를 올리면 상세 공시 정보가 표시됩니다.</div>
        </div>

        <script>
            var chart = klinecharts.init('kline-chart');
            var panel = document.getElementById('event-panel');
            
            chart.setStyles({{
                candle: {{
                    bar: {{
                        upColor: '#ef5350', downColor: '#26a69a', noChangeColor: '#888888',
                        upBorderColor: '#ef5350', downBorderColor: '#26a69a', noChangeBorderColor: '#888888',
                        upWickColor: '#ef5350', downWickColor: '#26a69a', noChangeWickColor: '#888888'
                    }},
                    tooltip: {{
                        // 차트 위 툴팁은 심플하게 가격만 띄우거나 최소화
                        custom: function(data) {{
                            var kline = data.current;
                            if (kline && kline.eventDetails) {{
                                var d = kline.eventDetails;
                                // 하단 패널에 HTML 쏴주기
                                panel.innerHTML = `
                                    <h4 class="evt-title" style="color: ${{d.color}};">🔔 ${{d.title}} (${{d.date}})</h4>
                                    <p class="evt-summary">${{d.summary}}</p>
                                    <p class="evt-extra">📊 ${{d.extra}}</p>
                                `;
                                return [{{ title: '이벤트', value: d.title, color: d.color }}];
                            }} else {{
                                // 공시가 없는 캔들에 마우스를 올리면 패널 초기화
                                panel.innerHTML = '<div class="empty-text">해당 일자에는 주요 공시 이벤트가 없습니다.</div>';
                                return [];
                            }}
                        }}
                    }}
                }}
            }});

            var chartData = {json.dumps(kline_data)};
            chart.applyNewData(chartData);
            chart.createIndicator('VOL', false, {{ height: 100 }});

            // 💡 마커 최적화: 캔들 시야를 가리지 않도록 아주 작고 심플하게
            var annotations = {json.dumps(annotations)};
            annotations.forEach(function(anno) {{
                chart.createOverlay({{
                    name: 'simpleAnnotation',
                    extendData: '',
                    points: [{{ timestamp: anno.timestamp, value: anno.value }}],
                    styles: {{
                        point: {{
                            show: true,
                            backgroundColor: anno.color,
                            borderColor: '#ffffff',
                            borderSize: 1,
                            radius: 3 // 크기를 확 줄임
                        }},
                        line: {{ show: false }},
                        text: {{ show: false }}
                    }}
                }});
            }});
        </script>
    </body>
    </html>
    """
    # 하단 패널이 추가되었으므로 전체 iframe 높이를 620 -> 750으로 늘려줍니다.
    components.html(html_code, height=750)

# ------------------------------------------
# [Tab 2] 퀀트 조건 스크리너 (LLM 스키마 100% 반영)
# ------------------------------------------
with tab2:
    st.subheader("🔎 LLM 정형 데이터 기반 퀀트 스크리너")
    
    col1, col2, col3 = st.columns(3)
    
    # 1. 영업이익 증감률 필터
    with col1:
        min_profit = -100
        if 'op_profit_change_pct' in final_df.columns:
            min_profit = st.slider("최소 영업이익 증감률 (%)", -100, 200, 10)
            
    # 2. 시가배당률 필터 (추가됨!)
    with col2:
        min_dividend = 0.0
        if 'dividend_yield' in final_df.columns:
            min_dividend = st.slider("최소 시가배당률 (%)", 0.0, 10.0, 3.0, step=0.5)
            
    # 3. 특정 이벤트 필터
    with col3:
        selected_events = []
        if 'event_type' in final_df.columns:
            available_events = final_df['event_type'].dropna().unique().tolist()
            selected_events = st.multiselect("특정 이벤트 포함", available_events)

    screened_df = final_df.copy()

    # 데이터 타입 변환 (문자열로 들어온 숫자들을 float로 변환)
    if 'op_profit_change_pct' in screened_df.columns:
        screened_df['op_profit_change_pct'] = pd.to_numeric(screened_df['op_profit_change_pct'], errors='coerce')
    if 'dividend_yield' in screened_df.columns:
        screened_df['dividend_yield'] = pd.to_numeric(screened_df['dividend_yield'], errors='coerce')

    # 필터링 적용 (조건을 만족하거나, 해당 값이 아예 없는 다른 유형의 공시이거나)
    if 'op_profit_change_pct' in screened_df.columns:
        screened_df = screened_df[(screened_df['op_profit_change_pct'].isna()) | (screened_df['op_profit_change_pct'] >= min_profit)]
        
    if 'dividend_yield' in screened_df.columns:
        screened_df = screened_df[(screened_df['dividend_yield'].isna()) | (screened_df['dividend_yield'] >= min_dividend)]
        
    if 'event_type' in screened_df.columns and selected_events:
        screened_df = screened_df[screened_df['event_type'].isin(selected_events)]

    # 빈 컬럼(NaN) 에러 방지를 위한 동적 컬럼 선택
    display_columns = ['corp_name', 'rcept_dt', 'clean_report_nm']
    optional_columns = ['event_type', 'op_profit_change_pct', 'dividend_yield', 'turnaround_status', 'summary', 'evidence_text']
    
    for col in optional_columns:
        if col in screened_df.columns:
            display_columns.append(col)

    st.success(f"조건을 만족하는 공시가 총 {len(screened_df)}건 검색되었습니다.")
    st.dataframe(screened_df[display_columns].sort_values(by='rcept_dt', ascending=False), use_container_width=True)

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