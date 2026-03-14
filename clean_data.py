"""
Brasileirão 2026 — Data Pipeline
Módulo 2: Limpeza e padronização dos dados brutos
"""

import os
import logging
import pandas as pd
from glob import glob

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "dados_brutos")
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")
os.makedirs(PROCESSED_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# Mapa: nome com UF (partidas) → nome oficial (tabela CBF)
NOME_MAP = {
    "Athletico PR": "Athletico Paranaense",
    "Atlético MG": "Atlético Mineiro",
    "Bahia BA": "Bahia",
    "Botafogo RJ": "Botafogo",
    "Chapecoense SC": "Chapecoense",
    "Corinthians SP": "Corinthians",
    "Coritiba PR": "Coritiba SAF",
    "Cruzeiro MG": "Cruzeiro",
    "Flamengo RJ": "Flamengo",
    "Fluminense RJ": "Fluminense",
    "Grêmio RS": "Grêmio",
    "Internacional RS": "Internacional",
    "Mirassol SP": "Mirassol",
    "Palmeiras SP": "Palmeiras",
    "Red Bull Bragantino SP": "Red Bull Bragantino",
    "Remo PA": "Remo",
    "Santos SP": "Santos Fc",
    "São Paulo SP": "São Paulo",
    "Vasco da Gama RJ": "Vasco da Gama Saf",
    "Vitória BA": "Vitória",
}


def latest_file(prefix: str) -> str:
    files = sorted(glob(os.path.join(RAW_DIR, f"{prefix}_*.csv")))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para: {prefix}")
    return files[-1]


def save_processed(df: pd.DataFrame, name: str) -> str:
    path = os.path.join(PROCESSED_DIR, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✔ Salvo: {os.path.basename(path)}  ({len(df)} linhas)")
    return path


def clean_tabela(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► Limpando tabela de classificação...")
    numeric = [
        "posicao",
        "pontos",
        "jogos",
        "vitorias",
        "empates",
        "derrotas",
        "gols_pro",
        "gols_contra",
        "saldo_gols",
        "aproveitamento",
    ]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["posicao", "time"])
    df = df.sort_values("posicao").reset_index(drop=True)
    log.info(f"  → {len(df)} times na tabela")
    return df


def clean_partidas(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info("► Limpando partidas...")
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")

    for col in ["gols_mandante", "gols_visitante", "rodada"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset=["ref"])

    # Normaliza nomes para bater com a tabela CBF
    df["mandante"] = df["mandante"].map(NOME_MAP).fillna(df["mandante"])
    df["visitante"] = df["visitante"].map(NOME_MAP).fillna(df["visitante"])

    df_finalizadas = df.dropna(subset=["gols_mandante", "gols_visitante"]).copy()

    df_finalizadas["resultado_mandante"] = df_finalizadas.apply(
        lambda r: (
            "V"
            if r["gols_mandante"] > r["gols_visitante"]
            else ("E" if r["gols_mandante"] == r["gols_visitante"] else "D")
        ),
        axis=1,
    )
    df_finalizadas["resultado_visitante"] = df_finalizadas.apply(
        lambda r: (
            "V"
            if r["gols_visitante"] > r["gols_mandante"]
            else ("E" if r["gols_mandante"] == r["gols_visitante"] else "D")
        ),
        axis=1,
    )
    df_finalizadas["total_gols"] = (
        df_finalizadas["gols_mandante"] + df_finalizadas["gols_visitante"]
    )

    log.info(f"  → {len(df)} partidas totais | {len(df_finalizadas)} finalizadas")
    return df, df_finalizadas


def clean_artilharia(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► Limpando artilharia...")
    df["gols"] = pd.to_numeric(df["gols"], errors="coerce")
    df = df.dropna(subset=["gols", "jogador"])
    df = df.sort_values(["gols", "jogador"], ascending=[False, True]).reset_index(
        drop=True
    )
    df["posicao"] = df.index + 1
    log.info(f"  → {len(df)} artilheiros")
    return df


def run():
    log.info("=" * 60)
    log.info("  Brasileirão 2026 Pipeline — Limpeza")
    log.info(f"  Entrada: {RAW_DIR}")
    log.info(f"  Saída:   {PROCESSED_DIR}")
    log.info("=" * 60)

    results = {}

    try:
        df_tabela = pd.read_csv(latest_file("tabela"))
        results["tabela"] = clean_tabela(df_tabela)
        save_processed(results["tabela"], "tabela")
    except Exception as e:
        log.error(f"  ✘ tabela: {e}")

    try:
        df_partidas = pd.read_csv(latest_file("partidas"))
        df_all, df_fin = clean_partidas(df_partidas)
        results["partidas"] = df_all
        results["partidas_finalizadas"] = df_fin
        save_processed(df_all, "partidas")
        save_processed(df_fin, "partidas_finalizadas")
    except Exception as e:
        log.error(f"  ✘ partidas: {e}")

    try:
        df_art = pd.read_csv(latest_file("artilharia"))
        results["artilharia"] = clean_artilharia(df_art)
        save_processed(results["artilharia"], "artilharia")
    except Exception as e:
        log.error(f"  ✘ artilharia: {e}")

    log.info("=" * 60)
    log.info(f"  Concluído. {len(results)} datasets processados.")
    log.info("=" * 60)
    return results


if __name__ == "__main__":
    run()
