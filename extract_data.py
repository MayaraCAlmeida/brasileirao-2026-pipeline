# extração de dados
# Tabela de classificação
# Artilharia
# Partidas
# Placares


import os
import re
import time
import logging
import pdfplumber
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "dados_brutos")
os.makedirs(RAW_DIR, exist_ok=True)

# caminho do PDF da CBF
PDF_PATH = os.path.join(BASE_DIR, "Tabela_Detalhada_BSA_2026.pdf")
# 

CBF_URL = "https://www.cbf.com.br/futebol-brasileiro/tabelas/campeonato-brasileiro/serie-a/2026"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def save_raw(df, name):
    ts = datetime.now().strftime("%Y%m%d")
    path = os.path.join(RAW_DIR, f"{name}_{ts}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✔ Salvo: {os.path.basename(path)}  ({len(df)} linhas)")
    return path


def get_cbf_html() -> str:
    """
    Abre o site da CBF com Playwright e retorna o HTML
    após o JavaScript renderizar a página. Usa stealth para evitar detecção de bot.
    """
    from playwright.sync_api import sync_playwright

    log.info(f"  → Iniciando Playwright para {CBF_URL}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1920,1080",
            ],
        )

        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            },
        )

        # Remove flags que denunciam automação
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en'] });
            window.chrome = { runtime: {} };
        """)

        page = ctx.new_page()

        # Bloqueia recursos desnecessários
        page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot}", lambda r: r.abort())
        page.route("**/analytics**", lambda r: r.abort())
        page.route("**/gtag**", lambda r: r.abort())
        page.route("**/facebook**", lambda r: r.abort())

        log.info("  → Navegando para o site da CBF...")
        try:
            page.goto(CBF_URL, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            log.warning(f"  ⚠ goto timeout/erro ({e}), tentando com networkidle...")
            try:
                page.goto(CBF_URL, wait_until="networkidle", timeout=90000)
            except Exception as e2:
                log.warning(f"  ⚠ networkidle também falhou ({e2}), usando HTML atual")

        # Aguarda a tabela aparecer
        log.info("  → Aguardando tabela renderizar...")
        try:
            page.wait_for_selector("table", timeout=20000)
            log.info("  ✔ Tabela encontrada no DOM")
        except Exception:
            log.warning("  ⚠ Timeout aguardando <table> — pode ser JS lento")

        # Scroll para garantir lazy-load
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        html = page.content()
        log.info(f"  → HTML obtido: {len(html)} chars")

        # Debug: salva HTML bruto para inspeção se necessário
        debug_path = os.path.join(BASE_DIR, "debug_cbf.html")
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"  → HTML salvo em: {debug_path}")

        browser.close()

    return html


# TABELA DE CLASSIFICAÇÃO

def extract_tabela(html: str = None) -> pd.DataFrame:
    log.info("► Tabela de classificação (CBF)...")

    if html is None:
        html = get_cbf_html()

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    log.info(f"  → {len(tables)} tabela(s) encontrada(s) no HTML")

    if not tables:
        # Tenta encontrar dados em divs caso o layout não use <table>
        log.warning("  ⚠ Nenhuma <table> encontrada — tentando divs...")
        rows = _extract_tabela_divs(soup)
        if rows:
            df = pd.DataFrame(rows)
            save_raw(df, "tabela")
            return df
        raise RuntimeError(
            "Tabela não encontrada. Verifique debug_cbf.html para inspecionar o HTML retornado."
        )

    # Pega a tabela com mais colunas (no caso, a classificação)
    tabela_tag = max(tables, key=lambda t: len(t.find_all("th")), default=None)
    log.info(f"  → Tabela selecionada: {len(tabela_tag.find_all('th'))} headers, {len(tabela_tag.find_all('tr'))} linhas")

    rows = []
    for tr in tabela_tag.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 9:
            continue

        # Extrai posição
        td0 = tds[0]
        strong = td0.find("strong")
        td0_text = td0.get_text(" ", strip=True)

        if strong:
            pos_m = re.search(r"(\d+)", strong.get_text())
            posicao = pos_m.group(1) if pos_m else ""
        else:
            pos_m = re.search(r"^\s*(\d+)", td0_text)
            posicao = pos_m.group(1) if pos_m else ""

        if not posicao:
            continue

        # Extrai nome do time
        link = td0.find("a")
        if link:
            nome = link.get_text(strip=True)
        else:
            # Remove números e símbolos do início
            nome = re.sub(r"^[\d\s\+\-\u25bc\u25b2]+", "", td0_text).strip()

        if not nome or len(nome) < 2:
            continue

        def num(i):
            return tds[i].get_text(strip=True) if len(tds) > i else None

        rows.append({
            "posicao":      posicao,
            "time":         nome,
            "pontos":       num(1),
            "jogos":        num(2),
            "vitorias":     num(3),
            "empates":      num(4),
            "derrotas":     num(5),
            "gols_pro":     num(6),
            "gols_contra":  num(7),
            "saldo_gols":   num(8),
            "cartoes_amar": num(9) if len(tds) > 9 else None,
            "cartoes_verm": num(10) if len(tds) > 10 else None,
            "aproveitamento": num(11) if len(tds) > 11 else None,
        })

    if not rows:
        raise RuntimeError(
            f"Tabela encontrada mas sem linhas válidas. "
            f"Verifique debug_cbf.html. Total TRs: {len(tabela_tag.find_all('tr'))}"
        )

    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} times extraídos: {df['time'].tolist()}")
    save_raw(df, "tabela")
    return df


def _extract_tabela_divs(soup: BeautifulSoup) -> list:
    """ tenta extrair tabela de divs caso o site use layout sem <table>."""
    rows = []
    # Procura por padrões comuns de classificação em divs
    for div in soup.find_all("div", class_=re.compile(r"classif|ranking|standing|tabela", re.I)):
        items = div.find_all("div", class_=re.compile(r"item|row|team|club", re.I))
        for i, item in enumerate(items, 1):
            text = item.get_text(" ", strip=True)
            nums = re.findall(r"\d+", text)
            if len(nums) >= 5:
                rows.append({
                    "posicao": i,
                    "time": re.sub(r"[\d\s]+", "", text.split()[0]).strip() or f"Time {i}",
                    "pontos": nums[0] if nums else None,
                    "jogos":  nums[1] if len(nums) > 1 else None,
                })
        if rows:
            log.info(f"  → {len(rows)} times via divs")
            break
    return rows


# ARTILHARIA

def extract_artilharia(html: str = None) -> pd.DataFrame:
    log.info("► Artilharia (CBF)...")

    if html is None:
        html = get_cbf_html()

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            pos = tds[0].get_text(strip=True)
            gols_text = tds[2].get_text(strip=True) if len(tds) > 2 else ""

            if not re.match(r"^\d+$", pos) or not re.match(r"^\d+$", gols_text):
                continue

            link = tds[1].find("a")
            raw = link.get_text(strip=True) if link else tds[1].get_text(strip=True)

            img = tds[3].find("img") if len(tds) > 3 else None
            if img and img.get("title"):
                clube = img["title"].strip()
                nome = raw[len(clube):].strip() if raw.startswith(clube) else re.sub(r"^.+?-[A-Z]{2}\s*", "", raw).strip()
            else:
                m = re.match(r"^(.+?-[A-Z]{2})\s*(.*?)$", raw)
                if m:
                    clube = m.group(1).strip()
                    nome = m.group(2).strip()
                else:
                    clube = tds[3].get_text(strip=True) if len(tds) > 3 else ""
                    nome = raw

            if not nome:
                nome = raw

            rows.append({
                "posicao": int(pos),
                "jogador": nome,
                "gols":    int(gols_text),
                "clube":   clube,
            })

    if not rows:
        log.warning("  ⚠ Artilharia não encontrada nas tabelas.")
        return pd.DataFrame(columns=["posicao", "jogador", "gols", "clube"])

    df = pd.DataFrame(rows).drop_duplicates(subset=["posicao", "jogador"])
    df = df[["posicao", "jogador", "gols", "clube"]].sort_values("posicao").reset_index(drop=True)
    log.info(f"  → {len(df)} artilheiros")
    save_raw(df, "artilharia")
    return df


# PARTIDAS (PDF oficial da CBF)

def extract_partidas() -> pd.DataFrame:
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

                    ref  = str(row[0] or "").strip()
                    rod  = str(row[1] or "").strip()
                    data = str(row[2] or "").strip()
                    hora = str(row[3] or "").strip()
                    jogo = str(row[4] or "").strip()
                    est  = str(row[5] or "").strip()
                    cid  = str(row[6] or "").strip()
                    uf   = str(row[7] or "").strip()

                    if not ref.isdigit() or not jogo or jogo == "None":
                        continue

                    rod_m = re.search(r"(\d+)", rod)
                    if rod_m:
                        rodada_atual = int(rod_m.group(1))

                    data_clean = data.split("\n")[0].strip()
                    hora_clean = hora if hora not in ["A def.", "None", ""] else None

                    m = re.match(r"^(.+?)\s+(\d+)\s+x\s+(\d+)\s+(.+)$", jogo)
                    if m:
                        mandante  = m.group(1).strip()
                        gols_m    = m.group(2)
                        gols_v    = m.group(3)
                        visitante = m.group(4).strip()
                    else:
                        m2 = re.match(r"^(.+?)\s+x\s+(.+)$", jogo)
                        if m2:
                            mandante  = m2.group(1).strip()
                            gols_m    = None
                            gols_v    = None
                            visitante = m2.group(2).strip()
                        else:
                            continue

                    rows.append({
                        "ref":            ref,
                        "rodada":         rodada_atual,
                        "data":           data_clean,
                        "hora":           hora_clean,
                        "mandante":       mandante,
                        "gols_mandante":  gols_m,
                        "gols_visitante": gols_v,
                        "visitante":      visitante,
                        "estadio":        est,
                        "cidade":         cid,
                        "uf":             uf,
                    })

    df = pd.DataFrame(rows)
    log.info(f"  {len(df)} jogos extraídos, {df['rodada'].nunique()} rodadas")
    save_raw(df, "partidas")
    return df


# PLACARES (Playwright — complementa o PDF)

def scrape_placares_cbf(df_partidas: pd.DataFrame, html: str = None) -> pd.DataFrame:
    log.info("► Atualizando placares via CBF...")

    df = df_partidas.copy()
    df["ref"] = df["ref"].astype(str).str.strip()
    placares_encontrados = 0

    try:
        if html is None:
            html = get_cbf_html()

        soup = BeautifulSoup(html, "html.parser")

        # Tentativa 1: data-jogo
        jogos_html = soup.find_all(attrs={"data-jogo": True})
        if jogos_html:
            for bloco in jogos_html:
                ref_html = str(bloco.get("data-jogo", "")).strip().lstrip("0") or "0"
                texto = bloco.get_text(" ", strip=True)
                m = re.search(r"(\d+)\s*[xX]\s*(\d+)", texto)
                if not m:
                    continue
                gm, gv = m.group(1), m.group(2)
                mask = df["ref"] == ref_html
                if mask.any():
                    df.loc[mask, "gols_mandante"] = gm
                    df.loc[mask, "gols_visitante"] = gv
                    placares_encontrados += int(mask.sum())
            log.info(f"  → data-jogo: {placares_encontrados} placares")

        # Tentativa 2: links / jogos / campeonato brasileiro
        if placares_encontrados == 0:
            for a in soup.find_all("a", href=re.compile(r"/jogos/campeonato-brasileiro/(\d+)")):
                m_ref = re.search(r"/jogos/campeonato-brasileiro/(\d+)", a["href"])
                if not m_ref:
                    continue
                ref_html = m_ref.group(1).lstrip("0") or "0"
                container = a.find_parent() or a
                texto = container.get_text(" ", strip=True)
                m_placar = re.search(r"(\d+)\s*[xX]\s*(\d+)", texto)
                if not m_placar:
                    continue
                gm, gv = m_placar.group(1), m_placar.group(2)
                mask = df["ref"] == ref_html
                if mask.any():
                    df.loc[mask, "gols_mandante"] = gm
                    df.loc[mask, "gols_visitante"] = gv
                    placares_encontrados += int(mask.sum())
            log.info(f"  → links: {placares_encontrados} placares")

        # Tentativa 3: match por nome de time
        if placares_encontrados == 0:
            texto_pagina = soup.get_text(" ")
            for _, row in df[df["gols_mandante"].isna()].iterrows():
                m1 = row["mandante"].split()[0]
                v1 = row["visitante"].split()[0]
                padrao = (
                    rf"{re.escape(m1)}[^0-9]{{0,40}}"
                    rf"(\d+)\s*[xX]\s*(\d+)"
                    rf"[^0-9]{{0,40}}{re.escape(v1)}"
                )
                m = re.search(padrao, texto_pagina, re.IGNORECASE)
                if m:
                    mask = df["ref"] == str(row["ref"])
                    df.loc[mask, "gols_mandante"] = m.group(1)
                    df.loc[mask, "gols_visitante"] = m.group(2)
                    placares_encontrados += 1
            log.info(f"  → texto: {placares_encontrados} placares")

    except Exception as e:
        log.warning(f"  ⚠ Falhou ao buscar placares: {e}")

    atualizados = df["gols_mandante"].notna().sum()
    log.info(
        f"  ✔ {placares_encontrados} novos placares via scraping | "
        f"{atualizados} partidas com placar no total"
    )
    return df


# MAIN

def run():
    log.info("=" * 60)
    log.info("  Brasileirão 2026 Pipeline — Extração")
    log.info(f"  Saída: {RAW_DIR}")
    log.info("=" * 60)

    results = {}
    html_cbf = None  # reutiliza o HTML entre tabela, artilharia e placares

    # Obtém HTML uma única vez (isso pra evita múltiplas requisições ao site)
    try:
        html_cbf = get_cbf_html()
    except Exception as e:
        log.error(f"  ✘ Falha ao obter HTML da CBF: {e}")
        log.error("  → Verifique se playwright está instalado: playwright install chromium")

    # Tabela
    try:
        results["tabela"] = extract_tabela(html_cbf)
    except Exception as e:
        log.error(f"  ✘ Erro em 'tabela': {e}")

    # Artilharia
    try:
        results["artilharia"] = extract_artilharia(html_cbf)
    except Exception as e:
        log.error(f"  ✘ Erro em 'artilharia': {e}")

    # Partidas (PDF)
    try:
        df_partidas = extract_partidas()

        # Atualiza placares com o mesmo HTML já obtido
        try:
            df_partidas = scrape_placares_cbf(df_partidas, html_cbf)
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
