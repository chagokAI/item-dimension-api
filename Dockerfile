FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app --home-dir /app app
COPY requirements.txt ./
# 기본 로컬/일반 클라우드 구성은 CPU다. PyPI 기본 torch는 Linux에서 CUDA 패키지까지 받는다.
ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu
RUN pip install --index-url "$TORCH_INDEX_URL" torch==2.14.1 torchvision==0.29.1 \
    && pip install -r requirements.txt
COPY --chown=app:app app ./app
RUN mkdir -p /app/.cache/huggingface && chown -R app:app /app/.cache
USER app
HEALTHCHECK --interval=10s --timeout=5s --start-period=180s --retries=6 \
    CMD ["python", "-m", "app.healthcheck"]
# Spring과의 서비스 연동은 Kafka Worker를 사용한다.
# HTTP 진단 서버는 uvicorn app.main:app --host 0.0.0.0 --port 8000으로 별도 실행할 수 있다.
CMD ["python", "-m", "app.worker"]
