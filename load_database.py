# Carga no PostgreSQL com upsert

import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# Colunas que devem ser inteiras
INT_COLS = {
    "gols_mandante",
    "gols_visitante",
    "total_gols",
    "rodada",
    "posicao",
    "pontos",
    "jogos",
    "vitorias",
    "empates",
    "derrotas",
    "gols_pro",
    "gols_contra",
    "cartoes_amar",
    "cartoes_verm",
    "jogos_recentes",
    "vitorias_recentes",
    "empates_recentes",
    "derrotas_recentes",
    "pontos_recentes",
}

# Colunas que devem ser string
STR_COLS = {
    "ref",
    "time",
    "jogador",
    "clube",
    "mandante",
    "visitante",
    "estadio",
    "cidade",
    "uf",
    "hora",
    "forma_str",
    "resultado_mandante",
    "resultado_visitante",
}


def fix_row(row: dict) -> dict:
    """Converte cada valor do dicionário para tipo Python nativo seguro."""
    out = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float) and (v != v):  # NaN check sem import
            out[k] = None
        elif k in INT_COLS:
            try:
                out[k] = int(v)
            except (ValueError, TypeError):
                out[k] = None
        elif k in STR_COLS:
            out[k] = str(v) if not (isinstance(v, float) and v != v) else None
        else:
            try:
                out[k] = v.item()  # np.generic → Python nativo
            except AttributeError:
                out[k] = v
    return out


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER','postgres')}:"
        f"{os.getenv('DB_PASSWORD','postgres')}@"
        f"{os.getenv('DB_HOST','localhost')}:"
        f"{os.getenv('DB_PORT','5432')}/"
        f"{os.getenv('DB_NAME','brasileirao_pipeline')}"
    )
    log.info(
        f"  Conectando: {os.getenv('DB_HOST','localhost')}/{os.getenv('DB_NAME','brasileirao_pipeline')}"
    )
    return create_engine(url, pool_pre_ping=True)


def execute_with_retry(engine, statement: str, max_attempts: int = 3):
    """Executa um statement SQL com retry em caso de deadlock."""
    for attempt in range(max_attempts):
        try:
            with engine.begin() as conn:
                conn.execute(text(statement))
            return
        except OperationalError as e:
            if "deadlock" in str(e).lower() and attempt < max_attempts - 1:
                wait = 2**attempt  # 1s, 2s, 4s...
                log.warning(
                    f"  ⚠ Deadlock detectado, tentativa {attempt + 1}/{max_attempts}. Aguardando {wait}s..."
                )
                time.sleep(wait)
            else:
                raise


def run_sql_file(engine, path: str):
    """Executa cada statement do arquivo SQL separadamente, com retry em deadlock."""
    if not os.path.exists(path):
        log.warning(f"  SQL não encontrado: {path}")
        return False
    with open(path, encoding="utf-8") as f:
        sql = f.read()
    for stmt in sql.split(";"):
        s = stmt.strip()
        if s:
            execute_with_retry(engine, s)
    log.info(f"  ✔ SQL executado: {os.path.basename(path)}")
    return True


def upsert_table(
    engine,
    df: pd.DataFrame,
    table_name: str,
    conflict_cols: list,
    chunk_size: int = 100,
):
    insp = inspect(engine)
    db_cols = {c["name"] for c in insp.get_columns(table_name)}
    df = df[[c for c in df.columns if c in db_cols]].copy()
    df = df.drop_duplicates(subset=conflict_cols, keep="last")

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    total = len(df)

    with engine.begin() as conn:
        for i in range(0, total, chunk_size):
            chunk = [
                fix_row(r)
                for r in df.iloc[i : i + chunk_size].to_dict(orient="records")
            ]
            stmt = insert(table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_cols,
                set_={c.key: c for c in stmt.excluded if c.key not in conflict_cols},
            )
            conn.execute(stmt)
    log.info(f"  ✔ {table_name}  ({total} linhas)")


def run():
    log.info("=" * 60)
    log.info("  Brasileirão 2026 Pipeline — Carga no banco")
    log.info("=" * 60)

    try:
        engine = get_engine()
    except Exception as e:
        log.error(f"  ✘ Falha na conexão: {e}")
        raise

    run_sql_file(engine, os.path.join(BASE_DIR, "create_tables.sql"))

    tabelas = [
        ("tabela", "tabela", ["time"]),
        ("partidas", "partidas", ["ref"]),
        ("partidas_finalizadas", "partidas_finalizadas", ["ref"]),
        ("team_stats", "team_stats", ["time"]),
        ("forma_recente", "forma_recente", ["time"]),
        ("artilharia", "artilharia", ["jogador", "clube"]),
        ("gols_por_rodada", "gols_por_rodada", ["rodada"]),
    ]

    sucesso = 0
    for filename, table, conflict in tabelas:
        path = os.path.join(PROCESSED_DIR, f"{filename}.csv")
        if not os.path.exists(path):
            log.warning(f"  ✘ Não encontrado: {filename}.csv")
            continue
        try:
            df = pd.read_csv(path)
            upsert_table(engine, df, table, conflict)
            sucesso += 1
        except Exception as e:
            log.error(f"  ✘ Erro em '{filename}': {e}")

    log.info("=" * 60)
    log.info(f"  Concluído. {sucesso}/{len(tabelas)} tabelas carregadas.")
    log.info("=" * 60)


if __name__ == "__main__":
    run()
