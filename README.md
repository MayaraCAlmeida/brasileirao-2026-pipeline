# Brasileirão Série A 2026 — Data Pipeline

## Visão Geral

Este projeto automatiza a coleta, processamento e visualização de dados do Campeonato Brasileiro Série A 2026. Extrai dados em tempo real da CBF via web scraping e leitura de PDF oficial, processa e carrega em banco PostgreSQL, e gera um dashboard HTML interativo com classificação, artilharia, estatísticas por time e resultados.

**[🔴 Ver dashboard ao vivo](https://mayaracalmeida.github.io/brasileirao-2026-pipeline)**

### Pipelines Disponíveis

| Pipeline | Descrição | Método de Coleta |
|---|---|---|
| **Classificação** | Posição, pontos, V/E/D, gols e aproveitamento oficial | Web scraping (CBF) |
| **Probabilidades** | Simulação Monte Carlo com 10.000 cenários | Web scraping + cálculo local |
| **Times** | Desempenho por ataque, defesa, casa/fora e performance score | Web scraping (CBF) |
| **Artilharia** | Ranking de artilheiros da competição | Web scraping (CBF) |
| **Partidas** | Placares, rodadas e média de gols por rodada | Extração de PDF oficial (CBF) |


### Fluxo de Processamento

1. **Extração** — Scraping do site CBF + leitura do PDF oficial
2. **Limpeza** — Padronização dos dados brutos com mapeamento de nomes (`NOME_MAP`)
3. **Transformação** — Feature engineering: `team_stats`, `forma_recente`, `gols_por_rodada`, `performance_score`
4. **Carga** — Upsert no PostgreSQL (`ON CONFLICT DO UPDATE`) — re-execuções são seguras
5. **Dashboard** — Geração do `brasileirao_dashboard.html` com gráficos interativos via Chart.js
6. **Publicação** — Deploy automático no GitHub Pages a cada execução

---

### Dependências de Desenvolvimento

```bash
pip install -r requirements.txt
```

---

## Estrutura do Projeto

```plaintext
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

## Arquitetura do Pipeline

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

---

## Tabelas no Banco de Dados

| Tabela | Descrição |
|---|---|
| `tabela` | Classificação oficial (posição, pontos, V/E/D, gols, aproveitamento) |
| `partidas` | Todos os jogos realizados |
| `partidas_finalizadas` | Jogos encerrados com placar e resultado calculado |
| `team_stats` | Stats agregadas por time: casa/fora, médias, performance score |
| `artilharia` | Ranking de artilheiros |
| `gols_por_rodada` | Média de gols por rodada |

O `load_database.py` usa **upsert** (`ON CONFLICT DO UPDATE`) em todas as tabelas — re-execuções são seguras, sem risco de duplicação.

---

## Como Executar

### Pré-requisitos

- Python 3.11+
- PostgreSQL rodando localmente ou em nuvem
- PDF da Tabela Detalhada da CBF salvo na raiz do projeto

### 1. Clonar o repositório

```bash
git clone https://github.com/MayaraCAlmeida/brasileirao-2026-pipeline.git
cd brasileirao-2026-pipeline
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Configurar `.env`

Copie o arquivo de exemplo e preencha com suas credenciais:

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

### 4. Criar as tabelas no banco

```bash
psql -U postgres -d brasileirao_pipeline -f create_tables.sql
```

### 5. Executar o Pipeline

```bash
# Pipeline completo agora
python scheduler.py --run-now

# Agendar execução diária (padrão: 06:00 BRT)
python scheduler.py

# Agendar em horário customizado
python scheduler.py --hour 8 --minute 30

# Rodar etapas individualmente
python extract_data.py
python clean_data.py
python transform_data.py
python load_database.py
python generate_dashboard.py
```

---

## CI/CD com GitHub Actions

O arquivo `.github/workflows/pipeline.yml` automatiza tudo:

- **Execução diária** às 09:00 UTC (06:00 BRT)
- **Trigger manual** via `workflow_dispatch`
- Após o pipeline, o dashboard é publicado automaticamente no **GitHub Pages**

### Configurar os Secrets no Repositório

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

## Métricas Calculadas

### Performance Score

Índice composto de 0 a 100 por time:

```
Score = (aproveitamento × 0.5) + (saldo_gols/jogo × 0.3) + (gols_pro/jogo × 0.2)
```

### Forma Recente

Sequência de resultados (V/E/D) dos últimos 5 jogos, com aproveitamento percentual.

### Análise Casa × Fora

Comparativo de vitórias, gols e aproveitamento jogando em casa versus fora.

---

## Queries Analíticas

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

## Monitoramento e Logs

- Logging estruturado via `pipeline.log`
- Retry automático com backoff exponencial para requisições
- Verificação de idempotência antes de reprocessar dados já carregados
- Contagem de registros processados vs. ignorados em cada execução

---

## Tecnologias

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

## Observações

- A CBF pode demorar algumas horas para atualizar o site após os jogos. O pipeline roda diariamente de manhã, garantindo que os dados do dia anterior estejam disponíveis.

---

*Dados extraídos do site oficial da CBF. Este projeto não tem vínculo com a Confederação Brasileira de Futebol.*

---

## Responsável Técnica

Desenvolvido por: **Mayara C. Almeida** 
