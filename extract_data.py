"""
Brasileirao 2026 - Data Pipeline
- Tabela de classificação: CBF (scraping)
- Artilharia: CBF (scraping)
- Partidas: PDF oficial da CBF (Tabela Detalhada)
- Placares: CBF (scraping) — complementa o PDF com resultados já disponíveis

Dependências: pip install requests beautifulsoup4 pdfplumber pandas
"""

import os
import re
import time
import logging
import requests
import pdfplumber
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "dados_brutos")
os.makedirs(RAW_DIR, exist_ok=True)

# ── Coloque o caminho do PDF da CBF aqui ──────────────────
PDF_PATH = os.path.join(BASE_DIR, "Tabela_Detalhada_BSA_2026.pdf")
# ─────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}
CBF_URL = "https://www.cbf.com.br/futebol-brasileiro/tabelas/campeonato-brasileiro/serie-a/2026"

# URL base para buscar placares por rodada
# A CBF usa esse padrão para retornar os jogos de cada rodada em JSON
CBF_RODADA_URL = (
    "https://www.cbf.com.br/futebol-brasileiro/campeonatos/jogos/serie-a/2026/{rodada}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    time.sleep(1)
    return BeautifulSoup(r.text, "html.parser")


def save_raw(df, name):
    ts = datetime.now().strftime("%Y%m%d")
    path = os.path.join(RAW_DIR, f"{name}_{ts}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✔ Salvo: {os.path.basename(path)}  ({len(df)} linhas)")
    return path


# ─────────────────────────────────────────────────────────
# TABELA DE CLASSIFICAÇÃO (CBF scraping)
# ─────────────────────────────────────────────────────────
def extract_tabela():
    log.info("► Tabela de classificação (CBF)...")
    soup = get_soup(CBF_URL)

    tabela_tag = max(
        soup.find_all("table"), key=lambda t: len(t.find_all("th")), default=None
    )
    if not tabela_tag:
        raise RuntimeError("Tabela não encontrada.")

    rows = []
    for tr in tabela_tag.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 12:
            continue

        link = tds[0].find("a")
        nome = (
            link.get_text(strip=True)
            if link
            else re.sub(r"[\d\+\-]+", "", tds[0].get_text(" ", strip=True)).strip()
        )
        strong = tds[0].find("strong")
        if strong:
            pos_m = re.search(r"(\d+)", strong.get_text())
            posicao = pos_m.group(1) if pos_m else ""
        else:
            td0_raw = tds[0].get_text()
            pos_m = re.match(r"^\s*(\d+)(?=[\+\-\s]|[A-Za-zÀ-ú])", td0_raw)
            posicao = pos_m.group(1) if pos_m else re.sub(r"\D.*", "", td0_raw).strip()

        def num(i):
            return tds[i].get_text(strip=True) if len(tds) > i else None

        rows.append(
            {
                "posicao": posicao,
                "time": nome,
                "pontos": num(1),
                "jogos": num(2),
                "vitorias": num(3),
                "empates": num(4),
                "derrotas": num(5),
                "gols_pro": num(6),
                "gols_contra": num(7),
                "saldo_gols": num(8),
                "cartoes_amar": num(9),
                "cartoes_verm": num(10),
                "aproveitamento": num(11),
            }
        )

    df = pd.DataFrame(rows)
    df = df[df["time"].str.len() > 1].reset_index(drop=True)
    save_raw(df, "tabela")
    return df


# ─────────────────────────────────────────────────────────
# ARTILHARIA (CBF scraping)
# ─────────────────────────────────────────────────────────
def extract_artilharia():
    log.info("► Artilharia (CBF)...")
    soup = get_soup(CBF_URL)

    rows = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != 5:
                continue

            pos = tds[0].get_text(strip=True)
            gols = tds[2].get_text(strip=True)

            if not re.match(r"^\d+$", pos) or not re.match(r"^\d+$", gols):
                continue

            link = tds[1].find("a")
            raw = link.get_text(strip=True) if link else tds[1].get_text(strip=True)

            img = tds[3].find("img")
            if img and img.get("title"):
                clube = img["title"].strip()
                nome = (
                    raw[len(clube) :].strip()
                    if raw.startswith(clube)
                    else re.sub(r"^.+?-[A-Z]{2}\s*", "", raw).strip()
                )
            else:
                m = re.match(r"^(.+?-[A-Z]{2})\s*(.*?)$", raw)
                if m:
                    clube = m.group(1).strip()
                    nome = m.group(2).strip()
                else:
                    clube = tds[3].get_text(strip=True)
                    nome = raw

            if not nome:
                nome = raw

            rows.append(
                {
                    "posicao": int(pos),
                    "jogador": nome,
                    "gols": int(gols),
                    "clube": clube,
                }
            )

    df = pd.DataFrame(rows).drop_duplicates(subset=["posicao", "jogador"])
    df = df.sort_values("posicao").reset_index(drop=True)
    save_raw(df, "artilharia")
    return df


# ─────────────────────────────────────────────────────────
# PARTIDAS (PDF oficial da CBF)
# ─────────────────────────────────────────────────────────
def extract_partidas():
    log.info(f"► Partidas (PDF: {os.path.basename(PDF_PATH)})...")

    if not os.path.exists(PDF_PATH):
        raise FileNotFoundError(
            f"PDF não encontrado em: {PDF_PATH}\n"
            "Baixe a Tabela Detalhada em cbf.com.br e salve nesse caminho."
        )

    rows = []
    rodada_atual = None

    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    if not row:
                        continue
                    row = list(row) + [None] * 10

                    ref = str(row[0] or "").strip()
                    rod = str(row[1] or "").strip()
                    data = str(row[2] or "").strip()
                    hora = str(row[3] or "").strip()
                    jogo = str(row[4] or "").strip()
                    est = str(row[5] or "").strip()
                    cid = str(row[6] or "").strip()
                    uf = str(row[7] or "").strip()

                    if not ref.isdigit() or not jogo or jogo == "None":
                        continue

                    rod_m = re.search(r"(\d+)", rod)
                    if rod_m:
                        rodada_atual = int(rod_m.group(1))

                    data_clean = data.split("\n")[0].strip()
                    hora_clean = hora if hora not in ["A def.", "None", ""] else None

                    m = re.match(r"^(.+?)\s+(\d+)\s+x\s+(\d+)\s+(.+)$", jogo)
                    if m:
                        mandante = m.group(1).strip()
                        gols_m = m.group(2)
                        gols_v = m.group(3)
                        visitante = m.group(4).strip()
                    else:
                        m2 = re.match(r"^(.+?)\s+x\s+(.+)$", jogo)
                        if m2:
                            mandante = m2.group(1).strip()
                            gols_m = None
                            gols_v = None
                            visitante = m2.group(2).strip()
                        else:
                            continue

                    rows.append(
                        {
                            "ref": ref,
                            "rodada": rodada_atual,
                            "data": data_clean,
                            "hora": hora_clean,
                            "mandante": mandante,
                            "gols_mandante": gols_m,
                            "gols_visitante": gols_v,
                            "visitante": visitante,
                            "estadio": est,
                            "cidade": cid,
                            "uf": uf,
                        }
                    )

    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} jogos extraídos, {df['rodada'].nunique()} rodadas")
    save_raw(df, "partidas")
    return df


# ─────────────────────────────────────────────────────────
# PLACARES VIA SCRAPING (CBF — complementa o PDF)
#
# Estratégia:
#   1. Itera pelas rodadas que já existem no PDF (1 até max_rodada)
#   2. Para cada rodada, acessa a página da CBF e tenta extrair
#      os placares das partidas finalizadas
#   3. Faz merge com o DataFrame do PDF usando ref como chave,
#      sobrescrevendo gols_mandante/gols_visitante onde encontrar
#
# A CBF exibe os jogos de cada rodada em blocos como:
#   <div class="clube-mandante"> ... </div>
#   <div class="placar"> 2 x 1 </div>
#   <div class="clube-visitante"> ... </div>
# O ref (número do jogo) aparece como atributo data-jogo ou no link.
# Como o HTML da CBF muda com frequência, a função tem dois níveis
# de fallback: extração por data-jogo e extração posicional.
# ─────────────────────────────────────────────────────────
def scrape_placares_cbf(df_partidas: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o DataFrame completo de partidas (vindo do PDF) e tenta
    atualizar gols_mandante / gols_visitante com os resultados já
    publicados no site da CBF.

    Retorna o DataFrame com os placares preenchidos onde disponíveis.
    """
    log.info("► Buscando placares no site da CBF...")

    df = df_partidas.copy()
    # Garante que ref é string para bater com o que o site retorna
    df["ref"] = df["ref"].astype(str).str.strip()

    rodadas_disponiveis = sorted(df["rodada"].dropna().unique().astype(int))
    placares_encontrados = 0

    for rodada in rodadas_disponiveis:
        url = CBF_RODADA_URL.format(rodada=rodada)
        try:
            soup = get_soup(url)
        except Exception as e:
            log.warning(f"  ⚠ Rodada {rodada}: erro ao acessar ({e})")
            continue

        # ── Tentativa 1: blocos com data-jogo ────────────────────
        # A CBF costuma marcar cada jogo com data-jogo="REF"
        jogos_html = soup.find_all(attrs={"data-jogo": True})

        if jogos_html:
            for bloco in jogos_html:
                ref_html = str(bloco.get("data-jogo", "")).strip()

                # Procura placar dentro do bloco — padrão: "2 x 1" ou "2-1"
                texto = bloco.get_text(" ", strip=True)
                m_placar = re.search(r"(\d+)\s*[xX\-]\s*(\d+)", texto)
                if not m_placar:
                    continue

                gm, gv = m_placar.group(1), m_placar.group(2)

                mask = df["ref"] == ref_html
                if mask.any():
                    df.loc[mask, "gols_mandante"] = gm
                    df.loc[mask, "gols_visitante"] = gv
                    placares_encontrados += mask.sum()

        else:
            # ── Tentativa 2: extração posicional por tabela ───────
            # Busca todas as tabelas/linhas com padrão "Time A X-Y Time B"
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue

                texto_linha = tr.get_text(" ", strip=True)

                # Verifica se há placar no formato "N x N" ou "N-N"
                m_placar = re.search(r"(\d+)\s*[xX\-]\s*(\d+)", texto_linha)
                if not m_placar:
                    continue

                # Tenta capturar o ref na linha (número isolado de 3 dígitos)
                m_ref = re.search(r"\b(0\d{2}|\d{3})\b", texto_linha)
                if not m_ref:
                    continue

                ref_html = m_ref.group(1).lstrip("0") or "0"
                gm, gv = m_placar.group(1), m_placar.group(2)

                mask = df["ref"] == ref_html
                if mask.any():
                    df.loc[mask, "gols_mandante"] = gm
                    df.loc[mask, "gols_visitante"] = gv
                    placares_encontrados += mask.sum()

        time.sleep(0.5)  # respeita o rate limit da CBF entre rodadas

    atualizados = df["gols_mandante"].notna().sum()
    log.info(
        f"  ✔ Placares: {placares_encontrados} atualizações via scraping | "
        f"{atualizados} partidas com placar no total"
    )
    return df


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("  Brasileirão 2026 Pipeline — Extração")
    log.info(f"  Saída: {RAW_DIR}")
    log.info("=" * 60)

    results = {}

    # Etapas independentes de scraping
    for name, func in [
        ("tabela", extract_tabela),
        ("artilharia", extract_artilharia),
    ]:
        try:
            results[name] = func()
        except Exception as e:
            log.error(f"  ✘ Erro em '{name}': {e}")

    # Partidas: PDF + complemento de placares via scraping
    try:
        df_partidas = extract_partidas()

        # Tenta enriquecer com placares do site (falha silenciosa: mantém o PDF)
        try:
            df_partidas = scrape_placares_cbf(df_partidas)
        except Exception as e:
            log.warning(f"  ⚠ scrape_placares_cbf falhou, usando só o PDF: {e}")

        save_raw(df_partidas, "partidas")
        results["partidas"] = df_partidas

    except Exception as e:
        log.error(f"  ✘ Erro em 'partidas': {e}")

    log.info("=" * 60)
    log.info(f"  Concluído. {len(results)}/3 etapas com sucesso.")
    log.info("=" * 60)
    return results


if __name__ == "__main__":
    run()
