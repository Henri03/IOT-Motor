# Dockerfile für Django-basierte IoT-Webanwendung

FROM python:3.11-slim

ENV PYTHONUNBUFFERED 1

WORKDIR /app

# System-Requirements für psycopg2 und andere libs
RUN apt-get update && apt-get install -y \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./src /app


CMD ["gunicorn", "iot_project.asgi:application", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]