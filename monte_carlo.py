# Probabilidades do Brasileirão 2026 via simulação de Monte Carlo

import os
import logging
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")

log = logging.getLogger(__name__)

N_SIMULACOES = 10_000


def _aproveitamento(row) -> float:
    """Retorna o aproveitamento do time como float entre 0 e 1."""
    jogos = row.get("jogos", 0) or 0
    if jogos == 0:
        return 0.4  # fallback: 40%
    pts = row.get("pontos", 0) or 0
    return min(max(pts / (jogos * 3), 0.05), 0.95)


def simular(df_tabela: pd.DataFrame, df_partidas_all: pd.DataFrame) -> pd.DataFrame:

    log.info(f"► Monte Carlo: {N_SIMULACOES} simulações...")

    # Partidas ainda não realizadas (sem placar)
    df_futuras = df_partidas_all[
        df_partidas_all["gols_mandante"].isna()
        | df_partidas_all["gols_visitante"].isna()
    ][["ref", "mandante", "visitante"]].copy()

    if df_futuras.empty:
        log.warning(
            "  Nenhuma partida futura encontrada — probabilidades baseadas na tabela atual."
        )

    # Aproveitamento de cada time
    tabela_idx = df_tabela.set_index("time")
    times = df_tabela["time"].tolist()

    apr = {
        row["time"]: _aproveitamento(row) for row in df_tabela.to_dict(orient="records")
    }

    # Contadores
    contagem = {
        "campeo": {t: 0 for t in times},
        "libertadores": {t: 0 for t in times},
        "sulamericana": {t: 0 for t in times},
        "rebaixamento": {t: 0 for t in times},
    }

    pontos_base = {
        row["time"]: int(row.get("pontos", 0) or 0)
        for row in df_tabela.to_dict(orient="records")
    }
    sg_base = {
        row["time"]: int(row.get("saldo_gols", 0) or 0)
        for row in df_tabela.to_dict(orient="records")
    }

    futuras_list = df_futuras.to_dict(orient="records")

    rng = np.random.default_rng(42)

    for _ in range(N_SIMULACOES):
        pts = pontos_base.copy()
        sg = sg_base.copy()

        for jogo in futuras_list:
            m = jogo["mandante"]
            v = jogo["visitante"]

            if m not in apr or v not in apr:
                continue

            pm = apr[m]
            pv = apr[v]
            total = pm + pv
            # Probabilidade de vitória do mandante, empate, vitória do visitante
            p_vm = pm / (total + 0.3)  # leve vantagem pra empate
            p_vv = pv / (total + 0.3)
            p_e = 1 - p_vm - p_vv

            resultado = rng.choice(["vm", "e", "vv"], p=[p_vm, p_e, p_vv])

            if resultado == "vm":
                pts[m] = pts.get(m, 0) + 3
                sg[m] = sg.get(m, 0) + 1
                sg[v] = sg.get(v, 0) - 1
            elif resultado == "e":
                pts[m] = pts.get(m, 0) + 1
                pts[v] = pts.get(v, 0) + 1
            else:
                pts[v] = pts.get(v, 0) + 3
                sg[v] = sg.get(v, 0) + 1
                sg[m] = sg.get(m, 0) - 1

        # Classifica pelo critério: pontos → saldo de gols
        classificacao = sorted(
            times, key=lambda t: (pts.get(t, 0), sg.get(t, 0)), reverse=True
        )

        for pos, time in enumerate(classificacao, start=1):
            if pos == 1:
                contagem["campeo"][time] += 1
            if pos <= 4:
                contagem["libertadores"][time] += 1
            if 5 <= pos <= 12:
                contagem["sulamericana"][time] += 1
            if pos >= 16:
                contagem["rebaixamento"][time] += 1

    # Monta resultado
    rows = []
    for time in times:
        pos_atual = tabela_idx.loc[time, "posicao"] if time in tabela_idx.index else 99
        rows.append(
            {
                "time": time,
                "posicao": int(pos_atual) if pd.notna(pos_atual) else 99,
                "pontos": pontos_base.get(time, 0),
                "prob_campeo": round(contagem["campeo"][time] / N_SIMULACOES * 100, 1),
                "prob_libertadores": round(
                    contagem["libertadores"][time] / N_SIMULACOES * 100, 1
                ),
                "prob_sulamericana": round(
                    contagem["sulamericana"][time] / N_SIMULACOES * 100, 1
                ),
                "prob_rebaixamento": round(
                    contagem["rebaixamento"][time] / N_SIMULACOES * 100, 1
                ),
            }
        )

    df_result = pd.DataFrame(rows).sort_values("posicao").reset_index(drop=True)
    log.info(f"  ✔ Monte Carlo concluído ({N_SIMULACOES} simulações)")
    return df_result


def run() -> pd.DataFrame:
    tabela_path = os.path.join(PROCESSED_DIR, "tabela.csv")
    partidas_path = os.path.join(PROCESSED_DIR, "partidas.csv")

    if not os.path.exists(tabela_path):
        raise FileNotFoundError(
            "tabela.csv não encontrado. Rode clean_data.py primeiro."
        )
    if not os.path.exists(partidas_path):
        raise FileNotFoundError(
            "partidas.csv não encontrado. Rode clean_data.py primeiro."
        )

    df_tabela = pd.read_csv(tabela_path)
    df_partidas = pd.read_csv(partidas_path)

    df_prob = simular(df_tabela, df_partidas)

    out = os.path.join(PROCESSED_DIR, "probabilidades.csv")
    df_prob.to_csv(out, index=False)
    log.info(f"  ✔ probabilidades.csv salvo ({len(df_prob)} times)")
    return df_prob


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    run()
