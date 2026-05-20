"""
DAG: brasileirao_pipeline
Orquestra o pipeline completo do Brasileirão Série A 2026.
           Roda diariamente às 06:00 BRT (09:00 UTC).

Fluxo:
    extract → clean → transform → load → dashboard
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Caminho base do projeto dentro do container
PROJECT_DIR = "/opt/airflow/brasileirao"

# Argumentos padrão aplicados a todas as tasks
default_args = {
    "owner": "mayara",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="brasileirao_pipeline",
    default_args=default_args,
    description="Pipeline de dados do Brasileirão Série A 2026",
    schedule_interval="0 9 * * *",  # 09:00 UTC = 06:00 BRT
    start_date=days_ago(1),
    catchup=False,
    tags=["brasileirao", "etl", "scraping"],
) as dag:

    # Extração do site da CBF e do PDF oficial
    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT_DIR} && python extract_data.py",
        doc_md="""
        **Extração**
        Faz scraping do site da CBF e extrai dados do PDF oficial.
        Gera os CSVs brutos em `dados_brutos/`.
        """,
    )

    # Limpeza dos dados extraídos
    clean = BashOperator(
        task_id="clean",
        bash_command=f"cd {PROJECT_DIR} && python clean_data.py",
        doc_md="""
        **Limpeza**
        Padroniza nomes de times via NOME_MAP e remove inconsistências.
        Gera os CSVs limpos em `dados_processados/`.
        """,
    )

    # Transformação dos dados limpos
    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT_DIR} && python transform_data.py",
        doc_md="""
        **Transformação**
        Feature engineering: team_stats, forma_recente,
        gols_por_rodada, performance_score.
        """,
    )

    # Carga no banco PostgreSQL usando upsert para evitar duplicação
    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_DIR} && python load_database.py",
        doc_md="""
        **Carga**
        Upsert no PostgreSQL (ON CONFLICT DO UPDATE).
        Re-execuções são seguras — sem duplicação.
        """,
    )

    # Geração do dashboard com Chart.js
    dashboard = BashOperator(
        task_id="dashboard",
        bash_command=f"cd {PROJECT_DIR} && python generate_dashboard.py",
        doc_md="""
        **Dashboard**
        Gera o brasileirao_dashboard.html com Chart.js.
        O GitHub Actions faz o deploy no Pages automaticamente.
        """,
    )

    # Dependências: pipeline linear
    extract >> clean >> transform >> load >> dashboard
