"""
Brasileirão 2026 — Data Pipeline
Scheduler: orquestra o pipeline completo e agenda execução diária

Uso:
  python scheduler.py --run-now          # executa agora
  python scheduler.py                    # agenda para 06:00 BRT todo dia
  python scheduler.py --hour 8 --minute 30
"""

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            str(Path(__file__).resolve().parent / "pipeline.log"), encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def run_full_pipeline():
    start = datetime.now()
    log.info("=" * 60)
    log.info(f"Brasileirão 2026 Pipeline — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        log.info("[1/5] Extraindo dados da CBF...")
        from extract_data import run as extract

        extract()

        log.info("[2/5] Limpando dados...")
        from clean_data import run as clean

        clean()

        log.info("[3/5] Transformando & calculando métricas...")
        from transform_data import run as transform

        transform()

        log.info("[4/5] Carregando no banco de dados...")
        from load_database import run as load

        load()

        log.info("[5/5] Gerando dashboard HTML...")
        from generate_dashboard import run as dash

        dash()

        elapsed = (datetime.now() - start).seconds
        log.info(f"✔ Pipeline concluído com sucesso em {elapsed}s")

    except Exception as e:
        log.error(f"✘ Pipeline FALHOU: {e}", exc_info=True)
        raise


def start_scheduler(hour: int = 6, minute: int = 0):
    scheduler = BlockingScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(
        run_full_pipeline,
        trigger=CronTrigger(hour=hour, minute=minute),
        id="brasileirao_daily",
        name="Brasileirão Daily Pipeline",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    log.info(
        f"Scheduler iniciado — pipeline roda diariamente às {hour:02d}:{minute:02d} BRT"
    )
    log.info("   Pressione Ctrl+C para parar.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler encerrado.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Brasileirão 2026 Pipeline Scheduler")
    parser.add_argument("--run-now", action="store_true", help="Executa imediatamente")
    parser.add_argument("--hour", type=int, default=6, help="Hora (BRT, 0-23)")
    parser.add_argument("--minute", type=int, default=0, help="Minuto (0-59)")
    args = parser.parse_args()

    if args.run_now:
        run_full_pipeline()
    else:
        start_scheduler(hour=args.hour, minute=args.minute)
