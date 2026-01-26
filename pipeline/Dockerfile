FROM python:3.13.11-slim
# 외부 도커 이미지(=uv 공식 이미지) 참조
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
# 이 ENV 환경변수를 추가하면, uv run 안해도 uv환경에서 실행됨
ENV PATH="/code/.venv/bin:$PATH"

# RUN pip install pyarrow pandas
# 3개의 파일을 current dir에 복사함
# 일일이 설치하지 않아도, 필요한 lib들을 자동 설치
COPY pyproject.toml .python-version uv.lock ./

# 동일한 의존성 설치
RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]