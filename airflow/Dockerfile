FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libnss3 \
    libpango-1.0-0 \
    libcairo2 \
    && apt-get clean

USER airflow
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt

RUN playwright install chromium
