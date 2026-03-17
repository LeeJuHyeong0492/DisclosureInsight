# 파이썬 3.11 슬림 버전 사용
FROM python:3.11-slim

WORKDIR /app

# psycopg2 설치를 위한 시스템 의존성 설치
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# 파이썬 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Streamlit 기본 포트 개방
EXPOSE 8501

# 컨테이너가 켜질 때 실행할 명령어
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]