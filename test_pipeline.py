# Testes do Brasileirão 2026 - Pipeline

import math
import pytest
import pandas as pd
import numpy as np

# Importações do pipeline
# tem q ajustar o path se necessário, ou colocar este arquivo na raiz do projeto
from clean_data import NOME_MAP, clean_tabela, clean_partidas, clean_artilharia
from transform_data import (
    compute_performance_score,
    build_team_stats,
    build_forma_recente,
    build_gols_por_rodada,
)
from load_database import fix_row

# FIXTURES


@pytest.fixture
def tabela_raw():
    """Tabela de classificação bruta com valores mistos."""
    return pd.DataFrame(
        {
            "posicao": [1, 2, None, 3],
            "time": ["Fluminense", "Flamengo", None, "Botafogo"],
            "pontos": ["20", "18", "15", "12"],
            "jogos": ["10", "10", "10", "10"],
            "vitorias": ["6", "5", "4", "3"],
            "empates": ["2", "3", "3", "3"],
            "derrotas": ["2", "2", "3", "4"],
            "gols_pro": ["18", "15", "12", "10"],
            "gols_contra": ["8", "9", "10", "12"],
            "saldo_gols": ["10", "6", "2", "-2"],
            "aproveitamento": ["66.7", "60.0", "50.0", "40.0"],
        }
    )


@pytest.fixture
def partidas_raw():
    """Partidas com nomes UF, gols e duplicata."""
    return pd.DataFrame(
        {
            "ref": ["p1", "p2", "p3", "p1"],  # p1 duplicado
            "rodada": [1, 1, 2, 1],
            "data": ["10/05/2026", "10/05/2026", "17/05/2026", "10/05/2026"],
            "mandante": [
                "Fluminense RJ",
                "Flamengo RJ",
                "Athletico PR",
                "Fluminense RJ",
            ],
            "visitante": ["Botafogo RJ", "São Paulo SP", "Atlético MG", "Botafogo RJ"],
            "gols_mandante": [2, 1, None, 2],
            "gols_visitante": [1, 1, None, 1],
        }
    )


@pytest.fixture
def partidas_finalizadas():
    """Partidas já finalizadas, prontas para transform."""
    return pd.DataFrame(
        {
            "ref": ["p1", "p2", "p3", "p4"],
            "rodada": [1, 1, 2, 2],
            "mandante": ["Fluminense", "Flamengo", "Botafogo", "Fluminense"],
            "visitante": ["Botafogo", "São Paulo", "Flamengo", "Corinthians"],
            "gols_mandante": [2, 1, 0, 3],
            "gols_visitante": [1, 1, 2, 0],
            "resultado_mandante": ["V", "E", "D", "V"],
            "resultado_visitante": ["D", "E", "V", "D"],
            "total_gols": [3, 2, 2, 3],
        }
    )


@pytest.fixture
def tabela_oficial():
    return pd.DataFrame(
        {
            "time": ["Fluminense", "Flamengo", "Botafogo", "São Paulo", "Corinthians"],
            "posicao": [1, 2, 3, 4, 5],
            "pontos": [6, 4, 3, 1, 0],
            "aproveitamento": [100.0, 66.7, 50.0, 33.3, 0.0],
        }
    )


# CLEAN_DATA


class TestNomeMap:
    def test_mapeamento_com_uf(self):
        assert NOME_MAP["Fluminense RJ"] == "Fluminense"

    def test_mapeamento_athletico(self):
        assert NOME_MAP["Athletico PR"] == "Athletico Paranaense"

    def test_mapeamento_atletico_mg(self):
        assert NOME_MAP["Atlético MG"] == "Atlético Mineiro"

    def test_todos_valores_sao_string(self):
        for k, v in NOME_MAP.items():
            assert isinstance(k, str) and isinstance(
                v, str
            ), f"Chave ou valor inválido: {k!r} → {v!r}"

    def test_sem_espacos_extras(self):
        for k, v in NOME_MAP.items():
            assert k == k.strip(), f"Chave com espaço extra: {k!r}"
            assert v == v.strip(), f"Valor com espaço extra: {v!r}"


class TestCleanTabela:
    def test_remove_linhas_sem_time(self, tabela_raw):
        df = clean_tabela(tabela_raw)
        assert df["time"].notna().all()

    def test_remove_linhas_sem_posicao(self, tabela_raw):
        df = clean_tabela(tabela_raw)
        assert df["posicao"].notna().all()

    def test_numericos_convertidos(self, tabela_raw):
        df = clean_tabela(tabela_raw)
        assert df["pontos"].dtype in [float, int, "int64", "float64"]
        assert df["aproveitamento"].dtype in [float, "float64"]

    def test_ordenado_por_posicao(self, tabela_raw):
        df = clean_tabela(tabela_raw)
        assert df["posicao"].is_monotonic_increasing

    def test_quantidade_apos_limpeza(self, tabela_raw):
        df = clean_tabela(tabela_raw)
        # PRA EU LEMBRAR QUE: a linha com time = None e posicao = None DEVE SER REMOVIDA !!!!!!!!!!!!!
        assert len(df) == 3


class TestCleanPartidas:
    def test_deduplicacao_por_ref(self, partidas_raw):
        df_all, _ = clean_partidas(partidas_raw)
        assert df_all["ref"].duplicated().sum() == 0

    def test_nome_map_aplicado_mandante(self, partidas_raw):
        df_all, _ = clean_partidas(partidas_raw)
        assert "Fluminense RJ" not in df_all["mandante"].values
        assert "Fluminense" in df_all["mandante"].values

    def test_nome_map_aplicado_visitante(self, partidas_raw):
        df_all, _ = clean_partidas(partidas_raw)
        assert "Botafogo RJ" not in df_all["visitante"].values

    def test_partidas_finalizadas_sem_gols_nulos(self, partidas_raw):
        _, df_fin = clean_partidas(partidas_raw)
        assert df_fin["gols_mandante"].notna().all()
        assert df_fin["gols_visitante"].notna().all()

    def test_resultado_mandante_vitoria(self, partidas_raw):
        _, df_fin = clean_partidas(partidas_raw)
        flu = df_fin[df_fin["mandante"] == "Fluminense"].iloc[0]
        assert flu["resultado_mandante"] == "V"

    def test_resultado_empate(self, partidas_raw):
        _, df_fin = clean_partidas(partidas_raw)
        fla = df_fin[df_fin["mandante"] == "Flamengo"].iloc[0]
        assert fla["resultado_mandante"] == "E"
        assert fla["resultado_visitante"] == "E"

    def test_total_gols_calculado(self, partidas_raw):
        _, df_fin = clean_partidas(partidas_raw)
        assert (
            df_fin["total_gols"] == df_fin["gols_mandante"] + df_fin["gols_visitante"]
        ).all()


class TestCleanArtilharia:
    def test_ordenado_decrescente(self):
        df = pd.DataFrame(
            {"gols": [5, 10, 3], "jogador": ["A", "B", "C"], "clube": ["X", "Y", "Z"]}
        )
        df_clean = clean_artilharia(df)
        assert df_clean.iloc[0]["jogador"] == "B"

    def test_remove_gols_nulos(self):
        df = pd.DataFrame(
            {
                "gols": [5, None, 3],
                "jogador": ["A", None, "C"],
                "clube": ["X", "Y", "Z"],
            }
        )
        df_clean = clean_artilharia(df)
        assert df_clean["gols"].notna().all()

    def test_posicao_começa_em_1(self):
        df = pd.DataFrame({"gols": [5, 3], "jogador": ["A", "B"], "clube": ["X", "Y"]})
        df_clean = clean_artilharia(df)
        assert df_clean["posicao"].iloc[0] == 1


# TRANSFORM_DATA


class TestPerformanceScore:
    def test_range_0_a_100(self):
        row = {
            "aproveitamento": 66.7,
            "saldo_gols_calc": 5,
            "total_gols_pro": 15,
            "total_jogos": 10,
        }
        score = compute_performance_score(row)
        assert 0 <= score <= 100

    def test_aproveitamento_zero(self):
        row = {
            "aproveitamento": 0,
            "saldo_gols_calc": -10,
            "total_gols_pro": 0,
            "total_jogos": 10,
        }
        score = compute_performance_score(row)
        assert score >= 0

    def test_aproveitamento_maximo(self):
        row = {
            "aproveitamento": 100,
            "saldo_gols_calc": 20,
            "total_gols_pro": 30,
            "total_jogos": 10,
        }
        score = compute_performance_score(row)
        assert score <= 100

    def test_time_melhor_score_que_pior(self):
        lider = {
            "aproveitamento": 100,
            "saldo_gols_calc": 20,
            "total_gols_pro": 25,
            "total_jogos": 10,
        }
        ultimo = {
            "aproveitamento": 10,
            "saldo_gols_calc": -15,
            "total_gols_pro": 5,
            "total_jogos": 10,
        }
        assert compute_performance_score(lider) > compute_performance_score(ultimo)

    def test_jogos_zero_nao_divide_por_zero(self):
        row = {
            "aproveitamento": 50,
            "saldo_gols_calc": 0,
            "total_gols_pro": 0,
            "total_jogos": 0,
        }
        score = compute_performance_score(row)
        assert not math.isnan(score)

    def test_valores_none_tratados(self):
        row = {
            "aproveitamento": None,
            "saldo_gols_calc": None,
            "total_gols_pro": None,
            "total_jogos": None,
        }
        score = compute_performance_score(row)
        assert 0 <= score <= 100


class TestBuildTeamStats:
    def test_retorna_dataframe(self, partidas_finalizadas, tabela_oficial):
        df = build_team_stats(partidas_finalizadas, tabela_oficial)
        assert isinstance(df, pd.DataFrame)

    def test_todos_times_presentes(self, partidas_finalizadas, tabela_oficial):
        df = build_team_stats(partidas_finalizadas, tabela_oficial)
        times = set(df["time"].values)
        # Fluminense jogou como mandante e visitante
        assert "Fluminense" in times

    def test_performance_score_entre_0_e_100(
        self, partidas_finalizadas, tabela_oficial
    ):
        df = build_team_stats(partidas_finalizadas, tabela_oficial)
        assert (df["performance_score"] >= 0).all()
        assert (df["performance_score"] <= 100).all()

    def test_saldo_gols_consistente(self, partidas_finalizadas, tabela_oficial):
        df = build_team_stats(partidas_finalizadas, tabela_oficial)
        flu = df[df["time"] == "Fluminense"].iloc[0]
        assert (
            flu["saldo_gols_calc"] == flu["total_gols_pro"] - flu["total_gols_contra"]
        )

    def test_total_jogos_correto_fluminense(self, partidas_finalizadas, tabela_oficial):
        df = build_team_stats(partidas_finalizadas, tabela_oficial)
        flu = df[df["time"] == "Fluminense"].iloc[0]
        # Fluminense: mandante em p1 e p4, visitante em nenhuma
        assert flu["total_jogos"] == 2


class TestBuildFormaRecente:
    def test_retorna_dataframe(self, partidas_finalizadas):
        df = build_forma_recente(partidas_finalizadas)
        assert isinstance(df, pd.DataFrame)

    def test_colunas_obrigatorias(self, partidas_finalizadas):
        df = build_forma_recente(partidas_finalizadas)
        for col in [
            "time",
            "jogos_recentes",
            "vitorias_recentes",
            "pontos_recentes",
            "forma_str",
        ]:
            assert col in df.columns

    def test_pontos_recentes_consistentes(self, partidas_finalizadas):
        df = build_forma_recente(partidas_finalizadas)
        for _, row in df.iterrows():
            esperado = row["vitorias_recentes"] * 3 + row["empates_recentes"]
            assert row["pontos_recentes"] == esperado

    def test_aproveitamento_entre_0_e_100(self, partidas_finalizadas):
        df = build_forma_recente(partidas_finalizadas)
        assert (df["aproveitamento_recente"] >= 0).all()
        assert (df["aproveitamento_recente"] <= 100).all()

    def test_forma_str_so_tem_v_e_d(self, partidas_finalizadas):
        df = build_forma_recente(partidas_finalizadas)
        for forma in df["forma_str"]:
            assert all(c in "VED" for c in forma)


class TestBuildGolsPorRodada:
    def test_retorna_dataframe(self, partidas_finalizadas):
        df = build_gols_por_rodada(partidas_finalizadas)
        assert isinstance(df, pd.DataFrame)

    def test_uma_linha_por_rodada(self, partidas_finalizadas):
        df = build_gols_por_rodada(partidas_finalizadas)
        assert df["rodada"].duplicated().sum() == 0

    def test_total_gols_correto(self, partidas_finalizadas):
        df = build_gols_por_rodada(partidas_finalizadas)
        rodada_1 = df[df["rodada"] == 1].iloc[0]
        # p1 (3 gols) + p2 (2 gols) = 5
        assert rodada_1["total_gols"] == 5

    def test_media_gols_positiva(self, partidas_finalizadas):
        df = build_gols_por_rodada(partidas_finalizadas)
        assert (df["media_gols"] > 0).all()


# LOAD_DATABASE — fix_row


class TestFixRow:
    def test_nan_vira_none(self):
        row = {"gols_mandante": float("nan")}
        assert fix_row(row)["gols_mandante"] is None

    def test_int_col_convertida(self):
        row = {"pontos": 20.0}
        assert fix_row(row)["pontos"] == 20
        assert isinstance(fix_row(row)["pontos"], int)

    def test_str_col_convertida(self):
        row = {"time": 123}
        assert fix_row(row)["time"] == "123"

    def test_none_permanece_none(self):
        row = {"posicao": None}
        assert fix_row(row)["posicao"] is None

    def test_int_invalido_vira_none(self):
        row = {"pontos": "abc"}
        assert fix_row(row)["pontos"] is None

    def test_numpy_int_convertido(self):
        row = {"jogos": np.int64(10)}
        result = fix_row(row)
        assert result["jogos"] == 10
        assert isinstance(result["jogos"], int)

    def test_numpy_float_convertido(self):
        row = {"aproveitamento": np.float64(66.7)}
        result = fix_row(row)
        assert abs(result["aproveitamento"] - 66.7) < 0.01

    def test_str_nan_vira_none(self):
        row = {"time": float("nan")}
        assert fix_row(row)["time"] is None

    def test_row_completa(self):
        row = {
            "time": "Fluminense",
            "pontos": np.int64(20),
            "aproveitamento": np.float64(66.7),
            "gols_pro": float("nan"),
        }
        result = fix_row(row)
        assert result["time"] == "Fluminense"
        assert result["pontos"] == 20
        assert result["gols_pro"] is None
