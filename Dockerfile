FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_ENV=production \
    APP_HOST=0.0.0.0 \
    APP_PORT=18743 \
    WAITRESS_HOST=0.0.0.0 \
    WAITRESS_PORT=18743 \
    PLAYWRIGHT_NO_SANDBOX=1 \
    PLAYWRIGHT_DISABLE_DEV_SHM_USAGE=1 \
    TZ=Europe/Rome

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./

RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt \
    && python -m playwright install --with-deps chromium

COPY app.py FoglioStileAssoSoftware.xsl ./
COPY templates ./templates
COPY scripts ./scripts
COPY ops ./ops

RUN mkdir -p /app/storage /app/logs

EXPOSE 18743

CMD ["python", "ops/serve.py"]
