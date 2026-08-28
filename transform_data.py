# Transformação & Feature Engineering

# Métricas calculadas:
#  - Performance Score por time (pontos, saldo, aproveitamento corrigido)
#  - Forma recente (últimas 5 rodadas)
#  - Analise de jogos em casa e fora
#  - Consistência ofensiva e defensiva
#  - Gols por rodada


import os
import logging
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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


# Métricas 
def compute_performance_score(row) -> float:
    """
    Performance Score = (aproveitamento × 0.5) + (saldo_gols_norm × 0.3) + (gols_pro_norm × 0.2)
    Normalizado por jogo, de 0 a 100.
    """
    # `row.get("aproveitamento", 0) or 0` não pega NaN - bool(nan) é
    # True, então "nan or 0" retorna nan, não 0. Isso é o que causou o
    # Santos virar 100 de perfomance score estando todo lascado no campeonato
    # o aproveitamento vinha NaN do merge (nome não batia) e
    # passava disfarçado de "verdadeiro" por esse `or`, e depois
    # min(100, nan) silenciosamente retornou 100 (comparação com nan é
    # sempre False, então o min "não troca" o valor). Agora chequei o NaN
    # explicitamente com pd.isna antes de aplicar o default. Santos com 100 nao da né
    apr = row.get("aproveitamento")
    apr = 0 if pd.isna(apr) else apr

    saldo = row.get("saldo_gols_calc")
    saldo = 0 if pd.isna(saldo) else saldo

    gols = row.get("total_gols_pro")
    gols = 0 if pd.isna(gols) else gols

    jogos = row.get("total_jogos", 1)
    jogos = 1 if pd.isna(jogos) else jogos
    jogos = max(jogos, 1)

    saldo_pg = saldo / jogos
    gols_pg = gols / jogos

    score = (apr * 0.5) + (saldo_pg * 5 + 50) * 0.3 + (gols_pg * 10) * 0.2

    # min/max com nan é traiçoeiro (comparação com nan é sempre
    # False), então chequei nan explicitamente antes de aplicar os limites,
    # em vez de confiar em min(100, score) para "capar" um valor inválido.
    if pd.isna(score):
        return 0.0
    return round(max(0, min(100, score)), 2)


def build_team_stats(df_fin: pd.DataFrame, df_tabela: pd.DataFrame) -> pd.DataFrame:
    """Agrega estatísticas por time a partir das partidas finalizadas."""
    # chaves reais: "mandante", "visitante", "resultado_mandante", "resultado_visitante"
    todos_times = pd.concat([df_fin["mandante"], df_fin["visitante"]]).dropna().unique()

    records = []
    for time in todos_times:
        casa = df_fin[df_fin["mandante"] == time]
        fora = df_fin[df_fin["visitante"] == time]

        todos = pd.concat(
            [
                casa.rename(
                    columns={
                        "gols_mandante": "gols_pro",
                        "gols_visitante": "gols_contra",
                        "resultado_mandante": "resultado",
                    }
                ),
                fora.rename(
                    columns={
                        "gols_visitante": "gols_pro",
                        "gols_mandante": "gols_contra",
                        "resultado_visitante": "resultado",
                    }
                ),
            ]
        )

        if todos.empty:
            continue

        records.append(
            {
                "time": time,
                # Casa
                "jogos_casa": len(casa),
                "vitorias_casa": (casa["resultado_mandante"] == "V").sum(),
                "empates_casa": (casa["resultado_mandante"] == "E").sum(),
                "derrotas_casa": (casa["resultado_mandante"] == "D").sum(),
                "gols_pro_casa": casa["gols_mandante"].sum(),
                "gols_contra_casa": casa["gols_visitante"].sum(),
                # Fora
                "jogos_fora": len(fora),
                "vitorias_fora": (fora["resultado_visitante"] == "V").sum(),
                "empates_fora": (fora["resultado_visitante"] == "E").sum(),
                "derrotas_fora": (fora["resultado_visitante"] == "D").sum(),
                "gols_pro_fora": fora["gols_visitante"].sum(),
                "gols_contra_fora": fora["gols_mandante"].sum(),
                # Geral
                "total_jogos": len(todos),
                "total_vitorias": (todos["resultado"] == "V").sum(),
                "total_empates": (todos["resultado"] == "E").sum(),
                "total_derrotas": (todos["resultado"] == "D").sum(),
                "total_gols_pro": todos["gols_pro"].sum(),
                "total_gols_contra": todos["gols_contra"].sum(),
                "saldo_gols_calc": todos["gols_pro"].sum() - todos["gols_contra"].sum(),
                "media_gols_pro": round(todos["gols_pro"].mean(), 2),
                "media_gols_contra": round(todos["gols_contra"].mean(), 2),
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["aproveitamento_calc"] = (
        (df["total_vitorias"] * 3 + df["total_empates"]) / (df["total_jogos"] * 3) * 100
    ).round(1)

    # Merge com tabela oficial - chave real é "time"
    if not df_tabela.empty:
        df = df.merge(
            df_tabela[["time", "posicao", "pontos", "aproveitamento"]],
            on="time",
            how="left",
        )

        # validação pós-merge. Se `clean_data.py` normalizar os
        # nomes corretamente, isso nunca deve disparar mas se um nome novo
        # aparecer (time subiu p série A, mudança de nome no site da CBF, etc.),
        # quero ver isso no log ANTES de virar um outlier no dashboard,
        # não descobrir olhando o gráfico publicado. Pq nem sempre posso olhar né
        sem_match = df.loc[df["posicao"].isna(), "time"].tolist()
        if sem_match:
            log.error(
                f"  ✘ Times sem correspondência em 'tabela' (nome não bateu "
                f"no merge — cheque NOME_MAP em clean_data.py): {sem_match}"
            )

    df["performance_score"] = df.apply(compute_performance_score, axis=1)
    df = df.sort_values("posicao").reset_index(drop=True)
    return df


def build_forma_recente(df_fin: pd.DataFrame, n_rodadas: int = 5) -> pd.DataFrame:
    """Forma dos últimos N jogos por time."""
    df_fin = df_fin.sort_values("rodada")
    ultima_rodada = df_fin["rodada"].max()
    corte = ultima_rodada - n_rodadas + 1

    df_recente = df_fin[df_fin["rodada"] >= corte].copy()

    todos_times = (
        pd.concat([df_recente["mandante"], df_recente["visitante"]]).dropna().unique()
    )

    records = []
    for time in todos_times:
        casa = df_recente[df_recente["mandante"] == time]
        fora = df_recente[df_recente["visitante"] == time]

        resultados = (
            casa["resultado_mandante"].tolist() + fora["resultado_visitante"].tolist()
        )

        v = resultados.count("V")
        e = resultados.count("E")
        d = resultados.count("D")
        jogos = len(resultados)

        records.append(
            {
                "time": time,
                "jogos_recentes": jogos,
                "vitorias_recentes": v,
                "empates_recentes": e,
                "derrotas_recentes": d,
                "pontos_recentes": v * 3 + e,
                "forma_str": "".join(resultados[-5:]),
                "aproveitamento_recente": (
                    round((v * 3 + e) / (jogos * 3) * 100, 1) if jogos > 0 else 0
                ),
            }
        )

    return pd.DataFrame(records)


def build_gols_por_rodada(df_fin: pd.DataFrame) -> pd.DataFrame:
    """Média de gols por rodada (tendência da competição)."""
    # a chave real é "ref", não "partida_id"; "total_gols" que já existe no módulo 2
    rodada_stats = (
        df_fin.groupby("rodada")
        .agg(
            jogos=("ref", "count"),
            total_gols=("total_gols", "sum"),
            media_gols=("total_gols", "mean"),
            jogos_com_gols=("total_gols", lambda x: (x > 0).sum()),
        )
        .reset_index()
    )
    rodada_stats["media_gols"] = rodada_stats["media_gols"].round(2)
    return rodada_stats


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("  Brasileirão 2026 Pipeline — Transformação")
    log.info(f"  Pasta: {PROCESSED_DIR}")
    log.info("=" * 60)

    results = {}

    try:
        df_fin = pd.read_csv(os.path.join(PROCESSED_DIR, "partidas_finalizadas.csv"))
        df_tabela = pd.read_csv(os.path.join(PROCESSED_DIR, "tabela.csv"))

        log.info("► Calculando stats por time (home/away)...")
        team_stats = build_team_stats(df_fin, df_tabela)
        team_stats.to_csv(os.path.join(PROCESSED_DIR, "team_stats.csv"), index=False)
        log.info(f"  ✔ team_stats.csv  ({len(team_stats)} times)")
        results["team_stats"] = team_stats

        log.info("► Calculando forma recente (últimas 5 rodadas)...")
        forma = build_forma_recente(df_fin)
        forma.to_csv(os.path.join(PROCESSED_DIR, "forma_recente.csv"), index=False)
        log.info(f"  ✔ forma_recente.csv  ({len(forma)} times)")
        results["forma_recente"] = forma

        log.info("► Calculando gols por rodada...")
        gols_rod = build_gols_por_rodada(df_fin)
        gols_rod.to_csv(os.path.join(PROCESSED_DIR, "gols_por_rodada.csv"), index=False)
        log.info(f"  ✔ gols_por_rodada.csv  ({len(gols_rod)} rodadas)")
        results["gols_por_rodada"] = gols_rod

    except Exception as e:
        log.error(f"  ✘ Erro na transformação: {e}", exc_info=True)

    log.info("=" * 60)
    log.info(f"  Concluído. {len(results)} datasets gerados.")
    log.info("=" * 60)
    return results


if __name__ == "__main__":
    run()
