FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    pdfplumber \
    playwright \
    requests \
    beautifulsoup4 \
    pandas \
    psycopg2-binary \
    "sqlalchemy<2.0" \
    python-dotenv

RUN playwright install chromium --with-deps 2>/dev/null || \
    python -m playwright install chromium