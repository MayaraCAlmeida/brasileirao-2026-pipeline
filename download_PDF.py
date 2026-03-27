from playwright.sync_api import sync_playwright
import requests
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

CBF_URL = "https://www.cbf.com.br/futebol-brasileiro/tabelas/campeonato-brasileiro/serie-a/2026"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_PATH = os.path.join(BASE_DIR, "Tabela_Detalhada_BSA_2026.pdf")


def download_pdf():
    log.info("► Buscando PDF da CBF...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(CBF_URL, wait_until="domcontentloaded")

        page.wait_for_selector("a[href$='.pdf']", timeout=15000)

        links = page.locator("a[href$='.pdf']").all()
        pdf_link = None
        for link in links:
            href = link.get_attribute("href") or ""
            if "tabela" in href.lower():
                pdf_link = href
                break

        if not pdf_link:
            pdf_link = page.locator("a[href$='.pdf']").first.get_attribute("href")

        browser.close()

    if not pdf_link:
        raise ValueError("Link da Tabela Detalhada não encontrado.")

    if pdf_link.startswith("/"):
        pdf_link = "https://www.cbf.com.br" + pdf_link

    log.info(f"  → PDF encontrado: {pdf_link}")

    os.makedirs(os.path.dirname(PDF_PATH) or ".", exist_ok=True)
    response = requests.get(pdf_link, timeout=30)
    response.raise_for_status()

    with open(PDF_PATH, "wb") as f:
        f.write(response.content)

    log.info(f"  ✔ PDF salvo em: {PDF_PATH}")


if __name__ == "__main__":
    print("Iniciando...")
    download_pdf()
    print("Finalizado!")
