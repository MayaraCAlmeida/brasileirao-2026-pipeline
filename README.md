# Brasileirão Série A 2026 — Data Pipeline

Pipeline automatizado de coleta, processamento e visualização de dados do Campeonato Brasileiro Série A 2026. Extrai dados em tempo real da CBF via web scraping e PDF oficial, processa e carrega em banco PostgreSQL, e gera um dashboard HTML interativo com classificação, artilharia, estatísticas por time e resultados.

---

# Dashboard

O dashboard é gerado automaticamente e publicado via **GitHub Pages** a cada execução do pipeline — atualizado todo dia às 06:00 BRT.

**[Dashboard](https://mayaracalmeida.github.io/brasileirao-2026-pipeline)**

---

# Estrutura do Projeto

```
brasileirao-2026-pipeline/
│
├── extract_data.py          # Extração: scraping CBF + leitura do PDF
├── clean_data.py            # Limpeza e padronização dos dados brutos
├── transform_data.py        # Feature engineering e métricas derivadas
├── load_database.py         # Carga no PostgreSQL com upsert
├── generate_dashboard.py    # Geração do dashboard HTML
├── scheduler.py             # Orquestrador e agendador do pipeline
│
├── create_tables.sql        # DDL das tabelas do banco
├── analytics_queries.sql    # Queries analíticas prontas para uso
│
├── Tabela_Detalhada_BSA_2026.pdf  # Tabela oficial da CBF (fonte das partidas)
├── brasileirao_dashboard.html     # Dashboard gerado (atualizado a cada run)
│
├── dados_brutos/            # CSVs brutos (gerados pelo extract_data.py)
├── dados_processados/       # CSVs limpos (gerados pelo clean_data.py)
│
├── .github/workflows/pipeline.yml  # CI/CD: execução diária + deploy no Pages
├── .env.example             # Exemplo de variáveis de ambiente
├── requirements.txt         # Dependências Python
└── pipeline.log             # Log de execuções
```

---

# Arquitetura do Pipeline

```
CBF Website  ──scraping──►  extract_data.py
PDF Oficial  ──pdfplumber──►      │
                                  ▼
                           clean_data.py
                           (limpeza + NOME_MAP)
                                  │
                                  ▼
                         transform_data.py
                     (team_stats, forma_recente,
                      gols_por_rodada, performance_score)
                                  │
                          ┌───────┴────────┐
                          ▼                ▼
                   load_database.py   generate_dashboard.py
                   (PostgreSQL upsert)  (HTML interativo)
                                             │
                                             ▼
                                      GitHub Pages
```

**Fontes de dados:**
- **Tabela de classificação** e **artilharia** — scraping do site `cbf.com.br`
- **Partidas** (placares, datas, estádios) — leitura do PDF oficial da CBF via `pdfplumber`

---

## Tabelas no Banco de Dados

| Tabela | Descrição |
|---|---|
| `tabela` | Classificação oficial (posição, pontos, V/E/D, gols, aproveitamento) |
| `partidas` | Todos os jogos (incluindo futuros, sem placar) |
| `partidas_finalizadas` | Jogos encerrados com placar e resultado calculado |
| `team_stats` | Stats agregadas por time: casa/fora, médias, performance score |
| `forma_recente` | Forma dos últimos 5 jogos por time |
| `artilharia` | Ranking de artilheiros |
| `gols_por_rodada` | Tendência de gols por rodada |

O `load_database.py` usa **upsert** (`ON CONFLICT DO UPDATE`) em todas as tabelas — re-execuções são seguras, sem risco de duplicação.

---

# Como Executar

# Pré-requisitos

- Python 3.11+
- PostgreSQL rodando localmente ou em nuvem
- PDF da Tabela Detalhada da CBF salvo na raiz do projeto

# Instalação

```bash
git clone https://github.com/MayaraCAlmeida/brasileirao-2026-pipeline.git
cd brasileirao-2026-pipeline
pip install -r requirements.txt
```

# Configuração

```bash
cp .env.example .env
```

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=brasileirao_pipeline
DB_USER=postgres
DB_PASSWORD=sua_senha
```

```bash
psql -U postgres -d brasileirao_pipeline -f create_tables.sql
```

# Execução

```bash
# Pipeline completo
python scheduler.py --run-now

# Execução diária agendada (padrão: 06:00 BRT)
python scheduler.py

# Horário customizado
python scheduler.py --hour 8 --minute 30

# Etapas individuais
python extract_data.py
python clean_data.py
python transform_data.py
python load_database.py
python generate_dashboard.py
```

---

# CI/CD com GitHub Actions

- **Execução diária** às 09:00 UTC (06:00 BRT)
- **Trigger manual** via `workflow_dispatch`
- Dashboard publicado automaticamente no **GitHub Pages** após cada run

# Secrets necessários

Vá em **Settings → Secrets and variables → Actions** e adicione:

| Secret | Descrição |
|---|---|
| `DB_HOST` | Host do PostgreSQL |
| `DB_USER` | Usuário do banco |
| `DB_PASSWORD` | Senha do banco |
| `DB_NAME` | Nome do banco |
| `DB_PORT` | Porta (geralmente `5432`) |

Ative o GitHub Pages em **Settings → Pages** apontando para a branch `gh-pages`.

---

# Métricas Calculadas

# Performance Score
Índice composto de 0 a 100 por time:
```
Score = (aproveitamento × 0.5) + (saldo_gols/jogo × 0.3) + (gols_pro/jogo × 0.2)
```

# Forma Recente
Sequência de resultados (V/E/D) dos últimos 5 jogos, com aproveitamento percentual.

# Análise Casa × Fora
Comparativo de vitórias, gols e aproveitamento jogando em casa versus fora.

# Simulação Monte Carlo
10.000 cenários simulados para estimar as probabilidades reais de cada time ser campeão, classificar para a Libertadores ou ser rebaixado.

---

# Queries Analíticas

O arquivo `analytics_queries.sql` contém 10 queries prontas, incluindo:

- Tabela de classificação completa
- Top 10 artilheiros
- Melhor ataque e melhor defesa
- Comparativo casa × fora
- Times em sequência positiva (3+ jogos sem perder)
- Tendência de gols por rodada
- Histórico de confrontos diretos entre dois times
- Ranking geral de performance

---

# Tecnologias

| Tecnologia | Uso |
|---|---|
| `requests` + `beautifulsoup4` | Scraping do site da CBF |
| `pdfplumber` | Extração de dados do PDF oficial |
| `pandas` + `numpy` | Processamento e transformação dos dados |
| `sqlalchemy` + `psycopg2` | ORM e conexão com PostgreSQL |
| `apscheduler` | Agendamento do pipeline |
| `Chart.js` | Gráficos no dashboard HTML |
| GitHub Actions | CI/CD e execução automática |
| GitHub Pages | Hospedagem do dashboard |

---

# Observações

- A CBF pode demorar algumas horas para atualizar o site após os jogos. O pipeline roda diariamente de manhã, garantindo que os dados do dia anterior estejam disponíveis.
- O PDF da Tabela Detalhada deve ser atualizado manualmente quando a CBF publicar uma nova versão (normalmente após alterações de datas ou locais).
- Todos os estádios listados estão sujeitos a alterações pela CBF. Consulte sempre a Tabela Detalhada oficial para informações definitivas.

---

# Versões

- **V1 (main):** Pipeline em Python puro com agendamento via GitHub Actions — [branch atual](https://github.com/MayaraCAlmeida/brasileirao-2026-pipeline)
- **V2 (em desenvolvimento):** Arquitetura de produção com orquestração via Apache Airflow, transformações modeladas com dbt e processamento distribuído com PySpark — [branch feat/airflow-dbt-spark](https://github.com/MayaraCAlmeida/brasileirao-2026-pipeline/tree/feat/airflow-dbt-spark)

---

*Dados extraídos do site oficial da CBF. Este projeto não tem vínculo com a Confederação Brasileira de Futebol.*  
*Desenvolvido por [Mayara C. Almeida](https://github.com/MayaraCAlmeida)*
