# Disclosure Insight: Agentic AI 기반 금융 공시 자동화 파이프라인 & 퀀트 대시보드

![Python](https://img.shields.io/badge/Python-3.10-blue) ![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-17a2b8?logo=apache-airflow) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql) ![AWS S3](https://img.shields.io/badge/AWS_S3-FF9900?logo=amazons3) ![OpenAI](https://img.shields.io/badge/OpenAI_GPT--4o--mini-412991?logo=openai) ![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit) ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?logo=github-actions)

## 💡 프로젝트 소개
다양한 형태(HTML, Text 등)로 존재하는 **DART(전자공시시스템)의 비정형 공시 원문 데이터를 안정적으로 수집하고, LLM을 활용해 분석 및 스크리닝이 용이한 정형 데이터(JSONB)로 변환하는 배치 파이프라인**입니다.

단순 데이터 적재를 넘어, 금융 데이터에서 치명적인 LLM의 '할루시네이션(환각)'을 방지하기 위해 **평가(Judge)와 교정(Healer) 에이전트를 도입한 다중 턴 피드백(Multi-turn Reflexion) 워크플로우**를 구축했습니다. 이를 통해 초기 데이터 추출 수율을 95% 이상으로 끌어올렸으며, 극소수의 예외 케이스는 Soft Fail 로직으로 방어하여 **파이프라인의 데이터 무결성(Integrity)을 100% 유지**했습니다. 정제된 데이터는 주가 백테스트가 가능한 **퀀트 대시보드**와 실시간으로 연동됩니다.

---

## 🏗️ 아키텍처 (Pipeline Architecture)

<img width="1264" height="842" alt="image" src="https://github.com/user-attachments/assets/305c9a1e-35f4-4bd1-ba7c-ae004ba65804" />


## 🔥 핵심 역량 및 트러블슈팅 (Key Features)

### 1. 🎯 비정형 데이터의 정형화 및 조건부 스크리닝 (Screening)
단순 키워드 검색만 가능했던 비정형 공시 텍스트를 LLM 프롬프트 엔지니어링을 통해 **관계형 DB의 JSONB 타입으로 정형화**했습니다. 이를 통해 수천 건의 공시 속에서 복잡한 금융 데이터 스크리닝 쿼리가 가능해졌습니다.

| 단계 | 데이터 형태 | 예시 및 특징 | 비즈니스 활용성 |
| :--- | :--- | :--- | :--- |
| **Before** | 비정형 (HTML/Text) | `"당사는 금번 이사회 결정에 따라 1주당 500원의..."` | ❌ 단순 문자열(키워드) 검색만 가능 |
| **After** | **정형 (JSONB)** | `{"dividend": 500, "yield": 4.2, "status": "PASS"}` | ✅ **수치 비교, 정렬, 다중 조건 필터링 가능** |

* **활용 가치:** "시가배당률 5% 이상 기업 필터링", "영업이익 100% 이상 상승 및 흑자전환 기업 감지" 등 즉각적인 퀀트 투자 인사이트 도출 가능.

### 2. 🤖 Agentic Workflow 기반의 데이터 자가 치유 (Self-Healing + Reflexion 기법)
* **문제:** 단일 LLM 프롬프트 사용 시 원문 숫자를 엉뚱한 항목에 맵핑(Context Swapping)하거나, 형식(Format)을 핑계로 정상 데이터를 오류로 판정하는 과적합(Overfitting) 발생.
* **해결:** 단순 Prompting을 넘어 **'Reflexion(자기 반성)' 기법이 적용된 4단계 다중 AI 에이전트 루프** 도입.
  1. `Extractor`: 맞춤형 프롬프트로 1차 데이터 추출
  2. `Judge`: 추출된 데이터와 원문 팩트를 교차 검증하고, 실패 시 **"어떤 값이 어떻게 틀렸는지" 구체적인 수정 가이드라인(Correction Guideline) 제공.**
  3. `Healer`: Judge의 가이드라인을 바탕으로 데이터를 재수정.
  4. `Re-judge`: **Healer가 수정한 데이터를 다시 평가하여 완벽히 교정되었는지 최종 팩트체크.** 통과하지 못할 경우, 새로운 가이드라인과 함께 Healer에게 다시 반려 (최대 2회 루프 반복).
* **성과:** 초기 1차 추출 수율 약 65%에서 **다중 턴 피드백을 통해 95.5% 이상으로 대폭 향상.** 남은 4.5%의 복잡한 표/예외 케이스는 파이프라인 중단 없이 {"error": "수동 확인 요망"} 태그를 달아 적재하는 **Soft Fail를** 적용하여 데이터 누락(Data Loss) 0% 달성.

### 3. 🛡️ DataOps: 멱등성(Idempotency) 보장 및 CI/CD 구축
* **DB 적재 안정성:** 배치 재실행(Retry) 시 데이터 중복을 막기 위해, 공시 고유번호(`rcept_no`)를 Primary Key로 설정하고 PostgreSQL의 `ON CONFLICT DO UPDATE` (UPSERT) 구문을 적용하여 완벽한 멱등성(Idempotency) 확보.
* **CI/CD 자동화:** GitHub Actions를 도입하여, `main` 브랜치 푸시 시 파이썬 문법 에러를 자동으로 검사하는 CI 파이프라인 구축.
* **모듈화(Modularization):** 단일 DAG 코드를 `dart_api`, `html_parser`, `llm_agent`, `db_utils` 등 기능별로 분리하여 코드 가독성 및 유지보수성 확보.

### 4. 📊 데이터 시각화 및 백테스트 대시보드 (Streamlit & Klinecharts)
* HTML/JS를 직접 렌더링하여 트레이딩뷰(TradingView) 수준의 부드러운 **Klinecharts 주가 캔들 차트** 구현.
* 차트 위 캔들 하단에 공시 발생 시점을 마커(Dot)로 표기하고, 호버(Tooltip) 시 AI가 요약한 공시 이벤트 연동.
* 공시 발표일(T) 기준 **T+3, T+5 주가 수익률 백테스트 계산기** 구현.

---

## 🛠️ 기술 스택 (Tech Stack)
* **Language:** Python 3.10, JavaScript (Klinecharts)
* **Data Processing:** Pandas, BeautifulSoup4, Regex
* **Orchestration:** Apache Airflow
* **Database & Cloud:** PostgreSQL 15, AWS S3 (boto3)
* **AI & NLP:** OpenAI API (GPT-4o-mini), Prompt Engineering (Reflexion)
* **Frontend/BI:** Streamlit
* **DevOps:** Docker, GitHub Actions, Git

---

## 📂 프로젝트 구조 (Directory Structure)
```text
DisclosureInsight/
├── .github/workflows/
│   └── airflow_ci.yml          # GitHub Actions CI 파이프라인
├── dags/
│   ├── dart_collection_dag.py  # 메인 파이프라인 DAG (Task 의존성 관리)
│   └── modules/                # 비즈니스 로직 모듈화
│       ├── dart_api.py         # DART API 목록 수집
│       ├── s3_utils.py         # AWS S3 업로드 로직
│       ├── html_parser.py      # 동적 viewDoc 파싱 및 텍스트 정제
│       ├── llm_agent.py        # Agentic Workflow (추출/검증/치유/재검증)
│       └── db_utils.py         # PostgreSQL UPSERT 적재
├── streamlit/                  
│   └── dashboard.py            # Streamlit & Klinecharts 시각화 및 백테스트
├── docker-compose.yml          # Airflow & PostgreSQL 컨테이너 환경
└── requirements.txt



