FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

RUN python -m venv /opt/venv

COPY requirements.txt /build/requirements.txt
RUN /opt/venv/bin/pip install --upgrade pip && \
    /opt/venv/bin/pip install -r /build/requirements.txt

FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 nightwatch && \
    useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin nightwatch

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY nightwatch /app/nightwatch
COPY requirements.txt /app/requirements.txt

RUN mkdir -p /app/runtime /app/qdrant_data && \
    chown -R nightwatch:nightwatch /app

USER nightwatch

EXPOSE 8000 9001

CMD ["uvicorn", "nightwatch.main:app", "--host", "0.0.0.0", "--port", "8000"]
