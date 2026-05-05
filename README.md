# Brasileirão Série A 2026 — Data Pipeline

> Pipeline de dados end-to-end com ingestão automatizada, processamento analítico, simulação estatística, persistência em banco relacional e publicação de dashboard interativo via CI/CD.

**[Dashboard](https://mayaracalmeida.github.io/brasileirao-2026-pipeline)**

---

# O que este projeto faz

Coleta dados em tempo real do Campeonato Brasileiro Série A 2026 diretamente das fontes oficiais da CBF (web scraping + PDF oficial), processa e enriquece com métricas derivadas, persiste em PostgreSQL com garantia de idempotência, e publica um dashboard HTML interativo atualizado automaticamente via GitHub Actions.

Todo o pipeline é executado sem intervenção manual.

---

# Pipelines

| Pipeline | Fonte | O que entrega |
|---|---|---|
| **Classificação** | Web scraping (CBF) | Posição, pontos, V/E/D, gols, aproveitamento oficial |
| **Probabilidades** | Web scraping + cálculo local | Simulação Monte Carlo (10.000 cenários) de título/rebaixamento |
| **Times** | Web scraping (CBF) | Performance score composto, casa vs. fora, ataque e defesa |
| **Artilharia** | Web scraping (CBF) | Ranking de goleadores da competição |
| **Partidas** | PDF oficial (CBF) | Placares, rodadas e tendência de gols |

---

# Arquitetura

# Implementação atual

```
CBF Website  ──scraping──►  extract_data.py
PDF Oficial  ──pdfplumber──►       │
                                   ▼
                            clean_data.py
                         (limpeza + NOME_MAP)
                                   │
                                   ▼
                          transform_data.py
                      (team_stats · forma_recente
                       gols_por_rodada · performance_score)
                                   │
                         ┌─────────┴──────────┐
                         ▼                    ▼
                  load_database.py    generate_dashboard.py
                (PostgreSQL · upsert)   (HTML · Chart.js)
                                              │
                                      GitHub Actions CI/CD
                                              │
                                        GitHub Pages
```

# Arquitetura cloud equivalente (AWS)

Este pipeline foi projetado com separação de camadas compatível com arquiteturas cloud. O mapeamento direto para AWS seria:

| Componente atual | Equivalente AWS | Função |
|---|---|---|
| `extract_data.py` | AWS Lambda / Glue Crawler | Ingestão e scraping |
| `dados_brutos/` (CSV) | Amazon S3 (raw layer) | Data Lake — camada bruta |
| `clean_data.py` + `transform_data.py` | AWS Glue ETL / PySpark (EMR) | Processamento e feature engineering |
| `dados_processados/` (CSV) | Amazon S3 (processed layer) | Data Lake — camada processada |
| `load_database.py` | Amazon RDS (PostgreSQL) | Persistência relacional gerenciada |
| `scheduler.py` | Apache Airflow (MWAA) | Orquestração e agendamento |
| `generate_dashboard.py` | Amazon QuickSight / S3 static hosting | Visualização |
| GitHub Actions | AWS CodePipeline + CodeBuild | CI/CD |

A separação entre extração, limpeza, transformação e carga segue o padrão **ELT em camadas (Bronze → Silver → Gold)**, escalável para volumes maiores sem refatoração da lógica de negócio.

---

# Fluxo de processamento

1. **Extração** — scraping do site CBF + leitura estruturada do PDF oficial com `pdfplumber`
2. **Limpeza** — padronização dos dados brutos com mapeamento canônico de nomes (`NOME_MAP`)
3. **Transformação** — feature engineering: `team_stats`, `forma_recente`, `gols_por_rodada`, `performance_score`
4. **Carga** — upsert no PostgreSQL (`ON CONFLICT DO UPDATE`); re-execuções são idempotentes
5. **Dashboard** — geração do `brasileirao_dashboard.html` com gráficos interativos
6. **Publicação** — deploy automático no GitHub Pages a cada execução do pipeline

---

# Métricas calculadas

# Performance Score (índice 0–100)

```
Score = (aproveitamento × 0.5) + (saldo_gols/jogo × 0.3) + (gols_pro/jogo × 0.2)
```

Permite ranquear times além da tabela de pontos — captura consistência ofensiva e defensiva.

# Forma Recente

Sequência de resultados (V/E/D) dos últimos 5 jogos com aproveitamento percentual do período.

# Análise Casa × Fora

Comparativo de vitórias, gols marcados e aproveitamento em jogos como mandante vs. visitante.

# Simulação Monte Carlo

10.000 cenários simulados para projetar probabilidade de título, classificação para Libertadores e rebaixamento — recalculado a cada execução.

---

# Banco de dados

| Tabela | Descrição |
|---|---|
| `tabela` | Classificação oficial (posição, pontos, V/E/D, gols, aproveitamento) |
| `partidas` | Todos os jogos registrados |
| `partidas_finalizadas` | Jogos encerrados com placar e resultado calculado |
| `team_stats` | Stats agregadas por time: casa/fora, médias, performance score |
| `artilharia` | Ranking de artilheiros |
| `gols_por_rodada` | Média de gols por rodada |

Todas as tabelas utilizam **upsert** (`ON CONFLICT DO UPDATE`) — sem duplicação mesmo em re-execuções.

---

# CI/CD

```yaml
# .github/workflows/pipeline.yml
schedule:
  - cron: '0 9 * * *'  # 09:00 UTC / 06:00 BRT, diariamente
```

A cada execução:
1. Pipeline roda completo (extração → transformação → carga → dashboard)
2. Dashboard atualizado é publicado automaticamente no GitHub Pages
3. Logs registram contagem de registros processados vs. ignorados

Trigger manual disponível via `workflow_dispatch`.

# Secrets necessários

Configure em **Settings → Secrets and variables → Actions**:

| Secret | Valor |
|---|---|
| `DB_HOST` | Host do PostgreSQL |
| `DB_USER` | Usuário |
| `DB_PASSWORD` | Senha |
| `DB_NAME` | Nome do banco |
| `DB_PORT` | Porta (padrão: `5432`) |

---

# Queries analíticas prontas

O arquivo `analytics_queries.sql` inclui 10 queries de uso imediato:

- Classificação completa com aproveitamento
- Top 10 artilheiros
- Melhor ataque e melhor defesa da competição
- Comparativo mandante × visitante
- Times em sequência positiva (3+ jogos sem perder)
- Tendência de gols por rodada
- Histórico de confrontos diretos entre dois times
- Ranking geral por performance score

---

# Monitoramento

- Logging estruturado em `pipeline.log`
- Retry automático com **backoff exponencial** em requisições HTTP
- Verificação de idempotência antes de reprocessar dados já carregados
- Contagem de registros processados vs. ignorados por execução

---

# Estrutura do projeto

```
brasileirao-2026-pipeline/
│
├── extract_data.py          # Ingestão: scraping CBF + leitura de PDF
├── clean_data.py            # Limpeza e padronização (NOME_MAP)
├── transform_data.py        # Feature engineering e métricas derivadas
├── load_database.py         # Carga no PostgreSQL com upsert
├── generate_dashboard.py    # Geração do dashboard HTML interativo
├── scheduler.py             # Orquestrador e agendador do pipeline
│
├── create_tables.sql        # DDL das tabelas
├── analytics_queries.sql    # 10 queries analíticas prontas
│
├── dados_brutos/            # Camada Bronze — CSVs brutos (extract)
├── dados_processados/       # Camada Silver — CSVs limpos (clean + transform)
│
├── .github/workflows/
│   └── pipeline.yml         # CI/CD: execução diária + deploy no Pages
├── .env.example
├── requirements.txt
└── pipeline.log
```

---

# Como executar localmente

```bash
# 1 - Clonar
git clone https://github.com/MayaraCAlmeida/brasileirao-2026-pipeline.git
cd brasileirao-2026-pipeline

# 2 - Instalar dependências
pip install -r requirements.txt

# 3 - Configurar variáveis de ambiente
cp .env.example .env
# edite .env com suas credenciais PostgreSQL

# 4 - Criar tabelas
psql -U postgres -d brasileirao_pipeline -f create_tables.sql

# 5 - Executar
python scheduler.py --run-now          # pipeline completo agora
python scheduler.py                    # agendar para 06:00 BRT diariamente
python scheduler.py --hour 8 --minute 30  # horário customizado

# Etapas individuais
python extract_data.py
python clean_data.py
python transform_data.py
python load_database.py
python generate_dashboard.py
```

---

# Stack

| Tecnologia | Uso |
|---|---|
| `requests` + `beautifulsoup4` | Web scraping (CBF) |
| `pdfplumber` | Extração estruturada de PDF oficial |
| `pandas` + `numpy` | Processamento, transformação e feature engineering |
| `sqlalchemy` + `psycopg2` | ORM e conexão PostgreSQL |
| `apscheduler` | Orquestração e agendamento local |
| `Chart.js` | Visualizações interativas no dashboard |
| GitHub Actions | CI/CD — execução automática e deploy |
| GitHub Pages | Hospedagem do dashboard |

---

> - Dados extraídos do site oficial da CBF. Projeto independente, sem vínculo com a Confederação Brasileira de Futebol.

**Desenvolvido por [Mayara C. Almeida](https://github.com/MayaraCAlmeida)**
