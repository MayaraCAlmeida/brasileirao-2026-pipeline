# Geração do Dashboard HTML

import os
import ssl
import json
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER','postgres')}:"
        f"{os.getenv('DB_PASSWORD','postgres')}@"
        f"{os.getenv('DB_HOST','localhost')}:"
        f"{os.getenv('DB_PORT','5432')}/"
        f"{os.getenv('DB_NAME','brasileirao_pipeline')}"
    )
    return create_engine(url, pool_pre_ping=True)


def load_data(engine):
    queries = {
        "tabela": "SELECT * FROM tabela ORDER BY posicao",
        "team_stats": "SELECT * FROM team_stats ORDER BY posicao",
        "forma": "SELECT * FROM forma_recente",
        "artilharia": "SELECT * FROM artilharia ORDER BY gols DESC LIMIT 15",
        "partidas": "SELECT * FROM partidas_finalizadas ORDER BY rodada DESC, ref",
        "gols_rodada": "SELECT * FROM gols_por_rodada ORDER BY rodada",
    }
    data = {}
    with engine.connect() as conn:
        for key, q in queries.items():
            data[key] = pd.read_sql(text(q), conn)

    prob_path = os.path.join(BASE_DIR, "dados_processados", "probabilidades.csv")
    if os.path.exists(prob_path):
        data["probabilidades"] = pd.read_csv(prob_path)
        log.info(
            f"  ✔ probabilidades.csv carregado ({len(data['probabilidades'])} times)"
        )
    else:
        log.warning(
            "  probabilidades.csv não encontrado — tentando rodar Monte Carlo agora..."
        )
        try:
            from monte_carlo import run as mc_run

            data["probabilidades"] = mc_run()
            log.info("  ✔ Monte Carlo executado como fallback no generate_dashboard")
        except Exception as mc_err:
            log.error(f"  ✘ Monte Carlo falhou: {mc_err}", exc_info=True)
            data["probabilidades"] = pd.DataFrame()
            log.warning("  Aba de probabilidades ficará vazia.")

    return data


def gerar_html(data: dict) -> str:
    def upper_time(records, *cols):
        """Converte para maiúsculo as colunas de nome de time informadas."""
        for r in records:
            for col in cols:
                if col in r and r[col]:
                    r[col] = str(r[col]).upper()
        return records

    tabela = upper_time(data["tabela"].to_dict(orient="records"), "time")
    team_stats = upper_time(data["team_stats"].to_dict(orient="records"), "time")
    artilharia = upper_time(data["artilharia"].to_dict(orient="records"), "clube")
    partidas = upper_time(
        data["partidas"].head(50).to_dict(orient="records"), "mandante", "visitante"
    )
    gols_rodada = data["gols_rodada"].to_dict(orient="records")
    probabilidades = upper_time(
        (
            data["probabilidades"].to_dict(orient="records")
            if not data["probabilidades"].empty
            else []
        ),
        "time",
    )

    forma_records = data["forma"].to_dict(orient="records")
    forma_map = {str(r["time"]).upper(): r.get("forma_str", "") for r in forma_records}
    forma_recente = upper_time(forma_records, "time")

    from datetime import datetime, timezone, timedelta

    BRT = timezone(timedelta(hours=-3))
    gerado_em = datetime.now(BRT).strftime("%d/%m/%Y às %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Brasileirão 2026 — Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --verde:   #009B3A;
    --amarelo: #F5C518;
    --azul:    #003087;
    --bg:      #0d1117;
    --card:    #161b22;
    --border:  #30363d;
    --text:    #e6edf3;
    --muted:   #8b949e;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }}
  .header {{
    background: linear-gradient(135deg, #003087 0%, #009B3A 50%, #F5C518 100%);
    padding: 24px 32px; display: flex; align-items: center; justify-content: space-between;
  }}
  .header h1 {{ font-size: 1.8rem; font-weight: 800; color: white; text-shadow: 0 2px 4px rgba(0,0,0,0.4); }}
  .header .meta {{ color: rgba(255,255,255,0.85); font-size: 0.85rem; text-align: right; }}
  .badge {{ display: inline-block; background: rgba(255,255,255,0.2); border: 1px solid rgba(255,255,255,0.4);
            border-radius: 20px; padding: 4px 14px; font-size: 0.75rem; color: white; margin-bottom: 4px; }}
  .tabs {{ display: flex; background: var(--card); border-bottom: 1px solid var(--border); padding: 0 32px; gap: 4px; overflow-x: auto; }}
  .tab-btn {{ padding: 14px 22px; cursor: pointer; border: none; background: none; color: var(--muted);
              font-size: 0.9rem; font-weight: 600; border-bottom: 3px solid transparent; transition: all 0.2s; white-space: nowrap; }}
  .tab-btn:hover {{ color: var(--text); }}
  .tab-btn.active {{ color: var(--amarelo); border-bottom-color: var(--amarelo); }}
  .content {{ padding: 28px 32px; max-width: 1400px; margin: 0 auto; }}
  .tab-panel {{ display: none; }}
  .tab-panel.active {{ display: block; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 28px; }}
  .kpi-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; text-align: center; }}
  .kpi-value {{ font-size: 2rem; font-weight: 800; color: var(--amarelo); line-height: 1.1; word-break: break-word; overflow-wrap: break-word; }}
  .kpi-value.kpi-lider {{ font-size: clamp(0.85rem, 2vw, 1.4rem); }}
  .kpi-label {{ font-size: 0.8rem; color: var(--muted); margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 22px; margin-bottom: 22px; }}
  .card h3 {{ font-size: 1rem; font-weight: 700; color: var(--text); margin-bottom: 16px; padding-bottom: 10px; border-bottom: 1px solid var(--border); text-transform: uppercase; letter-spacing: 0.5px; }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 900px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}
  .table-wrap {{ overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
  th {{ color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 0.72rem;
        letter-spacing: 0.5px; padding: 10px 12px; text-align: right; border-bottom: 1px solid var(--border); }}
  th:first-child, th:nth-child(2) {{ text-align: left; }}
  td {{ padding: 10px 12px; text-align: right; border-bottom: 1px solid rgba(48,54,61,0.5); vertical-align: middle; }}
  td:first-child, td:nth-child(2) {{ text-align: left; }}
  tr:hover {{ background: rgba(255,255,255,0.03); }}
  tr:last-child td {{ border-bottom: none; }}
  .pos {{ width: 30px; font-weight: 700; color: var(--muted); }}
  .pos-top4 {{ color: #238636; }}
  .pos-lib  {{ color: #da3633; }}
  .time-nome {{ font-weight: 600; }}
  .pts {{ font-weight: 800; color: var(--amarelo); font-size: 1rem; }}
  .forma {{ display: flex; gap: 3px; justify-content: flex-end; }}
  .forma-v {{ background: #238636; color: white; width: 20px; height: 20px; border-radius: 4px; font-size: 0.7rem; font-weight: 700; display: flex; align-items: center; justify-content: center; }}
  .forma-e {{ background: #5a5a5a; color: white; width: 20px; height: 20px; border-radius: 4px; font-size: 0.7rem; font-weight: 700; display: flex; align-items: center; justify-content: center; }}
  .forma-d {{ background: #da3633; color: white; width: 20px; height: 20px; border-radius: 4px; font-size: 0.7rem; font-weight: 700; display: flex; align-items: center; justify-content: center; }}
  .artilheiro-row {{ display: flex; align-items: center; gap: 12px; padding: 10px 0; border-bottom: 1px solid rgba(48,54,61,0.5); }}
  .artilheiro-rank {{ font-size: 1.1rem; font-weight: 800; color: var(--muted); width: 30px; text-align: center; }}
  .artilheiro-rank.top3 {{ color: var(--amarelo); }}
  .artilheiro-info {{ flex: 1; }}
  .artilheiro-nome {{ font-weight: 700; }}
  .artilheiro-time {{ font-size: 0.78rem; color: var(--muted); }}
  .artilheiro-gols {{ font-size: 1.4rem; font-weight: 800; color: var(--verde); }}
  .partida-card {{
    background: rgba(255,255,255,0.03); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 18px; margin-bottom: 10px;
  }}
  .partida-main {{
    display: flex; align-items: center; justify-content: space-between; gap: 12px;
  }}
  .partida-time {{ flex: 1; text-align: center; }}
  .partida-time.mandante {{ text-align: right; }}
  .partida-time.visitante {{ text-align: left; }}
  .partida-nome {{ font-weight: 700; font-size: 0.95rem; }}
  /* resultado ao lado do nome do time */
  .partida-resultado {{
    font-size: 0.75rem;
    font-weight: 600;
    margin-top: 2px;
  }}
  .res-vitoria  {{ color: #238636; }}
  .res-empate   {{ color: var(--muted); }}
  .res-derrota  {{ color: #da3633; }}
  .partida-placar {{ text-align: center; min-width: 90px; }}
  .placar {{ font-size: 1.5rem; font-weight: 900; color: var(--amarelo); }}
  .placar-rodada {{ font-size: 0.72rem; color: var(--muted); }}
  /* linha de meta-info: estádio / cidade-UF / hora */
  .partida-meta {{
    display: flex; align-items: center; justify-content: center; gap: 10px;
    margin-top: 8px; padding-top: 8px;
    border-top: 1px solid rgba(48,54,61,0.5);
    font-size: 0.72rem; color: var(--muted);
    flex-wrap: wrap;
  }}
  .partida-meta span {{ display: flex; align-items: center; gap: 3px; }}
  .chart-container {{ position: relative; height: 280px; }}
  .zona-libertadores {{ border-left: 3px solid #238636; }}
  .zona-sulamericana  {{ border-left: 3px solid #1f6feb; }}
  .zona-rebaixamento  {{ border-left: 3px solid #da3633; }}
  .prob-bar-wrap {{ display: flex; align-items: center; gap: 8px; }}
  .prob-bar {{ height: 10px; border-radius: 5px; min-width: 2px; transition: width 0.4s; }}
  .prob-val  {{ font-size: 0.82rem; font-weight: 700; min-width: 42px; text-align: right; }}
  .prob-campeao      {{ background: #F5C518; }}
  .prob-libertadores {{ background: #238636; }}
  .prob-sulamericana {{ background: #1f6feb; }}
  .prob-rebaixamento {{ background: #da3633; }}
  .legend-prob {{ display: flex; gap: 20px; flex-wrap: wrap; font-size: 0.78rem; color: var(--muted); margin-top: 14px; }}
  .legend-dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 5px; }}
  .nota-mc {{ font-size: 0.75rem; color: var(--muted); margin-top: 12px; font-style: italic; }}
  /* ── desempenho recente ── */
  .aprov-bar-wrap {{ display: flex; align-items: center; gap: 8px; }}
  .aprov-bar-bg {{ flex: 1; background: rgba(255,255,255,0.07); border-radius: 4px; height: 8px; overflow: hidden; }}
  .aprov-bar-fill {{ height: 100%; border-radius: 4px; transition: width 0.4s; }}
  .aprov-val {{ font-size: 0.82rem; font-weight: 700; min-width: 38px; text-align: right; }}
  .cartao-amar {{ color: #F5C518; font-weight: 800; }}
  .cartao-verm {{ color: #da3633; font-weight: 800; }}
  .aprov-geral {{ font-size: 0.75rem; color: var(--muted); }}
  .delta-pos {{ color: #238636; font-weight: 700; font-size: 0.8rem; }}
  .delta-neg {{ color: #da3633; font-weight: 700; font-size: 0.8rem; }}
  .delta-neu {{ color: var(--muted); font-weight: 700; font-size: 0.8rem; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="badge">⚽ 2026</div>
    <h1>🇧🇷 Brasileirão Série A 2026</h1>
  </div>
  <div class="meta">
    <div>Atualizado em</div>
    <div style="font-weight:700;font-size:1rem;color:white">{gerado_em}</div>
  </div>
</div>

<div class="tabs">
  <button class="tab-btn active" onclick="showTab('classificacao', this)">🏆 Classificação</button>
  <button class="tab-btn" onclick="showTab('probabilidades', this)">🎲 Probabilidades</button>
  <button class="tab-btn" onclick="showTab('times', this)">📊 Times</button>
  <button class="tab-btn" onclick="showTab('artilharia', this)">⚽ Artilharia</button>
  <button class="tab-btn" onclick="showTab('partidas', this)">🎯 Partidas</button>
</div>

<div class="content">

<div id="tab-classificacao" class="tab-panel active">
  <div class="kpi-grid" id="kpi-grid"></div>
  <div class="card">
    <h3>📋 Tabela de Classificação</h3>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>#</th><th>Time</th><th>Pts</th><th>J</th>
            <th>V</th><th>E</th><th>D</th>
            <th>GP</th><th>GC</th><th>SG</th>
            <th>%</th><th>Forma</th>
          </tr>
        </thead>
        <tbody id="tabela-body"></tbody>
      </table>
    </div>
    <div style="margin-top:14px;display:flex;gap:20px;flex-wrap:wrap;font-size:0.78rem;color:var(--muted)">
      <span>🟢 Top 6 — Libertadores</span>
      <span>🔵 7º-15º — Sul-Americana</span>
      <span>🔴 16º-20º — Rebaixamento</span>
    </div>
  </div>
</div>

<div id="tab-probabilidades" class="tab-panel">
  <div class="card">
    <h3>🎲 Probabilidades — Simulação Monte Carlo (10.000 cenários)</h3>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>#</th><th>Time</th><th>Pts</th>
            <th>🏆 Campeão</th><th>🟢 Libertadores</th>
            <th>🔵 Sul-Americana</th><th>🔴 Rebaixamento</th>
          </tr>
        </thead>
        <tbody id="prob-body"></tbody>
      </table>
    </div>
    <div class="legend-prob">
      <span><span class="legend-dot" style="background:#F5C518"></span>Campeão (Top 4)</span>
      <span><span class="legend-dot" style="background:#238636"></span>Libertadores (Top 6)</span>
      <span><span class="legend-dot" style="background:#1f6feb"></span>Sul-Americana (7º–15º)</span>
      <span><span class="legend-dot" style="background:#da3633"></span>Rebaixamento (16º–20º)</span>
    </div>
    <p class="nota-mc">
      * Probabilidades calculadas via simulação de Monte Carlo com 10.000 cenários para o restante da temporada.
        Campeão = Top 4 | Libertadores = Top 6 | Sul-Americana = 7º–15º | Rebaixamento = 16º–20º.
        A força de cada time é baseada no aproveitamento atual. Atualizado a cada execução do pipeline.
    </p>
  </div>
  <div class="grid-2">
    <div class="card">
      <h3>🏆 Chance de Título — G4 atual</h3>
      <div class="chart-container" style="height:180px"><canvas id="chart-prob-campeao"></canvas></div>
    </div>
    <div class="card">
      <h3>🔴 Risco de Rebaixamento — Z4 atual</h3>
      <div class="chart-container" style="height:180px"><canvas id="chart-prob-rebaixamento"></canvas></div>
    </div>
  </div>
  <div class="grid-2">
    <div class="card">
      <h3>🟢 Libertadores — G6 atual</h3>
      <div class="chart-container" style="height:240px"><canvas id="chart-prob-libertadores"></canvas></div>
    </div>
    <div class="card">
      <h3>🔵 Sul-Americana — 7º ao 15º atual</h3>
      <div class="chart-container" style="height:380px"><canvas id="chart-prob-sulamericana"></canvas></div>
    </div>
  </div>
</div>

<div id="tab-times" class="tab-panel">
  <div class="grid-2">
    <div class="card">
      <h3>⚔️ Ataque vs Defesa</h3>
      <div class="chart-container"><canvas id="chart-ataque-defesa"></canvas></div>
    </div>
    <div class="card">
      <h3>🏠 Casa vs Fora</h3>
      <div class="chart-container"><canvas id="chart-home-away"></canvas></div>
    </div>
  </div>
  <div class="card">
    <h3>📈 Performance Score</h3>
    <div class="chart-container" style="height:340px"><canvas id="chart-performance"></canvas></div>
  </div>
</div>

<div id="tab-artilharia" class="tab-panel">
  <div class="grid-2">
    <div class="card">
      <h3>🥇 Artilheiros</h3>
      <div id="artilharia-list"></div>
    </div>
    <div class="card">
      <h3>📊 Top 8 Artilheiros</h3>
      <div class="chart-container"><canvas id="chart-artilharia"></canvas></div>
    </div>
  </div>
</div>

<div id="tab-partidas" class="tab-panel">
  <div class="grid-2">
    <div class="card">
      <h3>📅 Últimas Partidas</h3>
      <div id="partidas-list"></div>
    </div>
    <div class="card">
      <h3>📈 Média de Gols por Rodada</h3>
      <div class="chart-container"><canvas id="chart-gols-rodada"></canvas></div>
    </div>
  </div>
<div id="tab-desempenho" class="tab-panel">
  <div class="card">
    <h3>🔥 Forma Recente — Últimos Jogos</h3>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>#</th><th>Time</th>
            <th>J</th><th>V</th><th>E</th><th>D</th>
            <th>Pts</th><th>Aprov. Recente</th><th>Aprov. Geral</th><th>Δ</th>
          </tr>
        </thead>
        <tbody id="desempenho-forma-body"></tbody>
      </table>
    </div>
    <p style="font-size:0.75rem;color:var(--muted);margin-top:10px">* Δ = aproveitamento recente menos aproveitamento geral. Positivo = time em alta.</p>
  </div>
  <div class="grid-2">
    <div class="card">
      <h3>🟡 Cartões Amarelos — Ranking</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>#</th><th>Time</th><th>🟡 Amarelos</th><th>🔴 Vermelhos</th></tr></thead>
          <tbody id="desempenho-cartoes-body"></tbody>
        </table>
      </div>
    </div>
    <div class="card">
      <h3>📊 Aproveitamento: Recente vs Geral</h3>
      <div class="chart-container" style="height:340px"><canvas id="chart-aprov-comparativo"></canvas></div>
    </div>
  </div>
</div>

</div>

<script>
const TABELA         = {json.dumps(tabela,         default=str)};
const TEAM_STATS     = {json.dumps(team_stats,     default=str)};
const FORMA_MAP      = {json.dumps(forma_map)};
const FORMA_RECENTE  = {json.dumps(forma_recente,  default=str)};
const ARTILHARIA     = {json.dumps(artilharia,     default=str)};
const PARTIDAS       = {json.dumps(partidas,       default=str)};
const GOLS_RODADA    = {json.dumps(gols_rodada,    default=str)};
const PROBABILIDADES = {json.dumps(probabilidades, default=str)};

let chartsRendered = {{}};

function showTab(name, btn) {{
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  btn.classList.add('active');
  if (name === 'probabilidades' && !chartsRendered.probabilidades) {{ renderProbabilidades(); chartsRendered.probabilidades = true; }}
  if (name === 'times'          && !chartsRendered.times)          {{ renderChartsTime();    chartsRendered.times = true; }}
  if (name === 'artilharia'     && !chartsRendered.artilharia)     {{ renderArtilharia();    chartsRendered.artilharia = true; }}
  if (name === 'partidas'       && !chartsRendered.partidas)       {{ renderPartidas();      chartsRendered.partidas = true; }}
  if (name === 'desempenho'     && !chartsRendered.desempenho)     {{ renderDesempenho();    chartsRendered.desempenho = true; }}
}}

function renderKPIs() {{
  const totalGols   = PARTIDAS.reduce((s,p) => s + (p.total_gols||0), 0);
  const lider       = TABELA[0] || {{}};
  const rodadaAtual = PARTIDAS.length > 0 ? Math.max(...PARTIDAS.map(p => p.rodada||0)) : '—';
  const kpis = [
    {{ label: 'Rodada Atual',     value: rodadaAtual }},
    {{ label: 'Jogos Realizados', value: PARTIDAS.length }},
    {{ label: 'Total de Gols',    value: totalGols }},
    {{ label: 'Líder',            value: lider.time || '—' }},
    {{ label: 'Pts do Líder',     value: lider.pontos || '—' }},
    {{ label: 'Média Gols/Jogo',  value: PARTIDAS.length ? (totalGols/PARTIDAS.length).toFixed(1) : '—' }},
  ];
  document.getElementById('kpi-grid').innerHTML = kpis.map(k =>
    `<div class="kpi-card"><div class="kpi-value${{k.label==='Líder'?' kpi-lider':''}}">${{k.value}}</div><div class="kpi-label">${{k.label}}</div></div>`
  ).join('');
}}

function renderTabela() {{
  document.getElementById('tabela-body').innerHTML = TABELA.map((t, i) => {{
    const pos = i + 1;
    let rowCls = '', posCls = 'pos';
    if (pos <= 6)                   {{ rowCls = 'zona-libertadores'; posCls += ' pos-top4'; }}
    else if (pos >= 16)             {{ rowCls = 'zona-rebaixamento'; posCls += ' pos-lib'; }}
    else if (pos >= 7 && pos <= 15) rowCls = 'zona-sulamericana';
    const forma = FORMA_MAP[t.time] || '';
    const sg    = t.saldo_gols;
    return `<tr class="${{rowCls}}">
      <td><span class="${{posCls}}">${{pos}}</span></td>
      <td><span class="time-nome">${{t.time}}</span></td>
      <td><span class="pts">${{t.pontos}}</span></td>
      <td>${{t.jogos}}</td><td>${{t.vitorias}}</td><td>${{t.empates}}</td><td>${{t.derrotas}}</td>
      <td>${{t.gols_pro}}</td><td>${{t.gols_contra}}</td>
      <td style="color:${{sg>=0?'#238636':'#da3633'}};font-weight:700">${{sg>0?'+':''}}${{sg}}</td>
      <td>${{t.aproveitamento||0}}%</td>
      <td>${{formaHtml(forma)}}</td>
    </tr>`;
  }}).join('');
}}

function formaHtml(str) {{
  if (!str) return '';
  return '<div class="forma">' + str.split('').slice(-5).map(c => {{
    const cls = c==='V'?'forma-v':c==='E'?'forma-e':'forma-d';
    return `<div class="${{cls}}">${{c}}</div>`;
  }}).join('') + '</div>';
}}

function probBar(val, cls) {{
  const w = Math.min(val, 100);
  return `<div class="prob-bar-wrap">
    <div class="prob-bar ${{cls}}" style="width:${{w * 1.5}}px"></div>
    <span class="prob-val">${{val}}%</span>
  </div>`;
}}

/* ── helpers para resultado ── */
function resCls(res) {{
  if (!res) return '';
  const r = String(res).trim().toUpperCase();
  if (r === 'V') return 'res-vitoria';
  if (r === 'E') return 'res-empate';
  if (r === 'D') return 'res-derrota';
  return '';
}}

function resLabel(res) {{
  if (!res) return '';
  const r = String(res).trim().toUpperCase();
  if (r === 'V') return '● Vitória';
  if (r === 'E') return '● Empate';
  if (r === 'D') return '● Derrota';
  return '';
}}

function renderProbabilidades() {{
  if (!PROBABILIDADES.length) {{
    document.getElementById('prob-body').innerHTML =
      '<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:24px">Dados de probabilidade ainda não disponíveis. Execute o pipeline completo.</td></tr>';
    return;
  }}

  document.getElementById('prob-body').innerHTML = PROBABILIDADES.map((p, i) => {{
    const pos = p.posicao || i + 1;
    let rowCls = '';
    if (pos <= 6)                   rowCls = 'zona-libertadores';
    else if (pos >= 16)             rowCls = 'zona-rebaixamento';
    else if (pos >= 7 && pos <= 15) rowCls = 'zona-sulamericana';
    return `<tr class="${{rowCls}}">
      <td><span class="pos">${{pos}}</span></td>
      <td><span class="time-nome">${{p.time}}</span></td>
      <td><span class="pts">${{p.pontos}}</span></td>
      <td>${{probBar(p.prob_campeao,      'prob-campeao')}}</td>
      <td>${{probBar(p.prob_libertadores, 'prob-libertadores')}}</td>
      <td>${{probBar(p.prob_sulamericana, 'prob-sulamericana')}}</td>
      <td>${{probBar(p.prob_rebaixamento, 'prob-rebaixamento')}}</td>
    </tr>`;
  }}).join('');

  const cfgBase = {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ color: '#8b949e' }}, grid: {{ color: '#30363d' }} }},
      y: {{ ticks: {{ color: '#e6edf3' }}, grid: {{ color: '#30363d' }} }}
    }}
  }};

  const sortedByPos = [...PROBABILIDADES].sort((a, b) => a.posicao - b.posicao);
  const topCampeao  = sortedByPos.slice(0, 4);
  const topLib      = sortedByPos.slice(0, 6);
  const topSul      = sortedByPos.slice(6, 15);
  const topReb      = sortedByPos.slice(15, 20);

  new Chart(document.getElementById('chart-prob-campeao'), {{
    type: 'bar',
    data: {{
      labels: topCampeao.map(t => t.time),
      datasets: [{{ label: '% Campeão', data: topCampeao.map(t => t.prob_campeao),
        backgroundColor: 'rgba(245,197,24,0.75)', borderColor: '#F5C518', borderWidth: 1, borderRadius: 6 }}]
    }},
    options: {{ ...cfgBase, indexAxis: 'y' }}
  }});

  new Chart(document.getElementById('chart-prob-rebaixamento'), {{
    type: 'bar',
    data: {{
      labels: topReb.map(t => t.time),
      datasets: [{{ label: '% Rebaixamento', data: topReb.map(t => t.prob_rebaixamento),
        backgroundColor: 'rgba(218,54,51,0.75)', borderColor: '#da3633', borderWidth: 1, borderRadius: 6 }}]
    }},
    options: {{ ...cfgBase, indexAxis: 'y' }}
  }});

  new Chart(document.getElementById('chart-prob-libertadores'), {{
    type: 'bar',
    data: {{
      labels: topLib.map(t => t.time),
      datasets: [{{ label: '% Libertadores', data: topLib.map(t => t.prob_libertadores),
        backgroundColor: 'rgba(35,134,54,0.75)', borderColor: '#238636', borderWidth: 1, borderRadius: 6 }}]
    }},
    options: {{ ...cfgBase, indexAxis: 'y' }}
  }});

  new Chart(document.getElementById('chart-prob-sulamericana'), {{
    type: 'bar',
    data: {{
      labels: topSul.map(t => t.time),
      datasets: [{{ label: '% Sul-Americana', data: topSul.map(t => t.prob_sulamericana),
        backgroundColor: 'rgba(31,111,235,0.75)', borderColor: '#1f6feb', borderWidth: 1, borderRadius: 6 }}]
    }},
    options: {{ ...cfgBase, indexAxis: 'y' }}
  }});
}}

function renderChartsTime() {{
  const top10 = TEAM_STATS.slice(0, 10);
  const nomes = top10.map(t => t.time);
  const cfg   = {{ responsive:true, maintainAspectRatio:false,
    plugins:{{legend:{{labels:{{color:'#e6edf3'}}}}}},
    scales:{{
      x:{{ticks:{{color:'#8b949e',maxRotation:45}},grid:{{color:'#30363d'}}}},
      y:{{ticks:{{color:'#8b949e'}},grid:{{color:'#30363d'}}}}
    }}
  }};

  new Chart(document.getElementById('chart-ataque-defesa'), {{
    type:'bar', data:{{
      labels: nomes,
      datasets:[
        {{label:'Gols Marcados', data:top10.map(t=>t.total_gols_pro),    backgroundColor:'rgba(0,155,58,0.7)',  borderColor:'#009B3A', borderWidth:1}},
        {{label:'Gols Sofridos', data:top10.map(t=>t.total_gols_contra), backgroundColor:'rgba(218,54,51,0.7)', borderColor:'#da3633', borderWidth:1}},
      ]
    }}, options: cfg
  }});

  const cfgPct = JSON.parse(JSON.stringify(cfg));
  cfgPct.scales.y.ticks = {{ color:'#8b949e', callback: v => v+'%' }};
  cfgPct.scales.y.max   = 100;
  new Chart(document.getElementById('chart-home-away'), {{
    type:'bar', data:{{
      labels: nomes,
      datasets:[
        {{label:'% Vitórias Casa', data:top10.map(t=>t.jogos_casa?(t.vitorias_casa/t.jogos_casa*100).toFixed(1):0), backgroundColor:'rgba(245,197,24,0.7)', borderColor:'#F5C518', borderWidth:1}},
        {{label:'% Vitórias Fora', data:top10.map(t=>t.jogos_fora?(t.vitorias_fora/t.jogos_fora*100).toFixed(1):0), backgroundColor:'rgba(0,48,135,0.7)',   borderColor:'#003087', borderWidth:1}},
      ]
    }}, options: cfgPct
  }});

  const sorted = [...TEAM_STATS].sort((a,b)=>b.performance_score-a.performance_score).slice(0,15);
  new Chart(document.getElementById('chart-performance'), {{
    type:'bar', data:{{
      labels: sorted.map(t=>t.time),
      datasets:[{{ label:'Performance Score', data:sorted.map(t=>t.performance_score),
        backgroundColor: sorted.map((_,i)=>`hsl(${{120-i*7}},70%,50%)`), borderRadius:6 }}]
    }},
    options:{{ indexAxis:'y', responsive:true, maintainAspectRatio:false,
      plugins:{{legend:{{display:false}}}},
      scales:{{ x:{{ticks:{{color:'#8b949e'}},grid:{{color:'#30363d'}}}}, y:{{ticks:{{color:'#e6edf3'}},grid:{{color:'#30363d'}}}} }}
    }}
  }});
}}

function renderArtilharia() {{
  document.getElementById('artilharia-list').innerHTML = ARTILHARIA.slice(0,10).map((a,i) => {{
    const rankCls = i<3?'artilheiro-rank top3':'artilheiro-rank';
    const medal   = i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1;
    return `<div class="artilheiro-row">
      <div class="${{rankCls}}">${{medal}}</div>
      <div class="artilheiro-info">
        <div class="artilheiro-nome">${{a.jogador}}</div>
        <div class="artilheiro-time">${{a.clube}}</div>
      </div>
      <div class="artilheiro-gols">${{a.gols}} ⚽</div>
    </div>`;
  }}).join('');

  const top8 = ARTILHARIA.slice(0,8);
  new Chart(document.getElementById('chart-artilharia'), {{
    type:'bar', data:{{
      labels: top8.map(a=>a.jogador.split(' ').slice(-1)[0]),
      datasets:[{{ label:'Gols', data:top8.map(a=>a.gols),
        backgroundColor: top8.map((_,i)=>`hsl(${{i*40}},70%,55%)`), borderRadius:6 }}]
    }},
    options:{{ responsive:true, maintainAspectRatio:false,
      plugins:{{legend:{{display:false}}}},
      scales:{{ x:{{ticks:{{color:'#e6edf3'}},grid:{{color:'#30363d'}}}}, y:{{ticks:{{color:'#8b949e'}},grid:{{color:'#30363d'}}}} }}
    }}
  }});
}}

function renderPartidas() {{
  document.getElementById('partidas-list').innerHTML = PARTIDAS.slice(0,15).map(p => {{

    /* ── meta-info: estádio, cidade/UF, hora ── */
    const estadio  = p.estadio  || null;
    const cidade   = p.cidade   || null;
    const uf       = p.uf       || null;
    const hora     = p.hora     || null;

    const localStr = [cidade, uf].filter(Boolean).join('/');

    let metaParts = [];
    if (estadio)  metaParts.push(`<span>🏟️ ${{estadio}}</span>`);
    if (localStr) metaParts.push(`<span>📍 ${{localStr}}</span>`);
    if (hora)     metaParts.push(`<span>🕐 ${{hora}}</span>`);

    const metaRow = metaParts.length
      ? `<div class="partida-meta">${{metaParts.join('<span style="color:var(--border)">|</span>')}}</div>`
      : '';

    /* ── resultado de cada time (V / E / D) ── */
    const resMand = p.resultado_mandante || null;
    const resVisi = p.resultado_visitante || null;

    const resMandHtml = resMand
      ? `<div class="partida-resultado ${{resCls(resMand)}}">${{resLabel(resMand)}}</div>`
      : '';
    const resVisiHtml = resVisi
      ? `<div class="partida-resultado ${{resCls(resVisi)}}">${{resLabel(resVisi)}}</div>`
      : '';

    return `
    <div class="partida-card">
      <div class="partida-main">
        <div class="partida-time mandante">
          <div class="partida-nome">${{p.mandante}}</div>
          ${{resMandHtml}}
        </div>
        <div class="partida-placar">
          <div class="placar">${{p.gols_mandante}} × ${{p.gols_visitante}}</div>
          <div class="placar-rodada">Rodada ${{p.rodada}}</div>
        </div>
        <div class="partida-time visitante">
          <div class="partida-nome">${{p.visitante}}</div>
          ${{resVisiHtml}}
        </div>
      </div>
      ${{metaRow}}
    </div>`;
  }}).join('');

  if (GOLS_RODADA.length > 0) {{
    new Chart(document.getElementById('chart-gols-rodada'), {{
      type:'line', data:{{
        labels: GOLS_RODADA.map(r=>'Rd '+r.rodada),
        datasets:[{{ label:'Média de Gols', data:GOLS_RODADA.map(r=>r.media_gols),
          borderColor:'#F5C518', backgroundColor:'rgba(245,197,24,0.1)',
          fill:true, tension:0.4, pointRadius:4, pointBackgroundColor:'#F5C518' }}]
      }},
      options:{{ responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{labels:{{color:'#e6edf3'}}}}}},
        scales:{{ x:{{ticks:{{color:'#8b949e'}},grid:{{color:'#30363d'}}}}, y:{{ticks:{{color:'#8b949e'}},grid:{{color:'#30363d'}}}} }}
      }}
    }});
  }}
}}

function renderDesempenho() {{
  /* ── aproveitamento geral via TABELA ── */
  const aprovGeralMap = {{}};
  TABELA.forEach(t => {{
    aprovGeralMap[t.time] = parseFloat(t.aproveitamento) || 0;
  }});

  /* ── tabela de forma recente ── */
  const formaSorted = [...FORMA_RECENTE].sort((a, b) => {{
    const pa = (a.pontos_recentes || 0) / Math.max(a.jogos_recentes || 1, 1);
    const pb = (b.pontos_recentes || 0) / Math.max(b.jogos_recentes || 1, 1);
    return pb - pa;
  }});

  document.getElementById('desempenho-forma-body').innerHTML = formaSorted.map((f, i) => {{
    const j  = f.jogos_recentes    || 0;
    const v  = f.vitorias_recentes || 0;
    const e  = f.empates_recentes  || 0;
    const d  = f.derrotas_recentes || 0;
    const pt = f.pontos_recentes   || 0;
    const aprovRec   = j > 0 ? ((pt / (j * 3)) * 100) : 0;
    const aprovGeral = aprovGeralMap[f.time] || 0;
    const delta      = aprovRec - aprovGeral;
    const deltaCls   = delta > 2 ? 'delta-pos' : delta < -2 ? 'delta-neg' : 'delta-neu';
    const deltaStr   = (delta > 0 ? '+' : '') + delta.toFixed(1) + '%';

    /* barra de aproveitamento recente com cor dinâmica */
    const barColor = aprovRec >= 60 ? '#238636' : aprovRec >= 40 ? '#F5C518' : '#da3633';

    return `<tr>
      <td><span class="pos">${{i + 1}}</span></td>
      <td><span class="time-nome">${{f.time}}</span></td>
      <td>${{j}}</td><td>${{v}}</td><td>${{e}}</td><td>${{d}}</td>
      <td><span class="pts">${{pt}}</span></td>
      <td>
        <div class="aprov-bar-wrap">
          <div class="aprov-bar-bg">
            <div class="aprov-bar-fill" style="width:${{aprovRec.toFixed(1)}}%;background:${{barColor}}"></div>
          </div>
          <span class="aprov-val">${{aprovRec.toFixed(1)}}%</span>
        </div>
      </td>
      <td><span class="aprov-geral">${{aprovGeral.toFixed(1)}}%</span></td>
      <td><span class="${{deltaCls}}">${{deltaStr}}</span></td>
    </tr>`;
  }}).join('');

  /* ── cartões — vem de TABELA (cartoes_amar / cartoes_verm) ── */
  const cartoesSorted = [...TABELA].sort((a, b) => (b.cartoes_amar || 0) - (a.cartoes_amar || 0));

  document.getElementById('desempenho-cartoes-body').innerHTML = cartoesSorted.map((t, i) => `
    <tr>
      <td><span class="pos">${{i + 1}}</span></td>
      <td><span class="time-nome">${{t.time}}</span></td>
      <td><span class="cartao-amar">🟡 ${{t.cartoes_amar || 0}}</span></td>
      <td><span class="cartao-verm">🔴 ${{t.cartoes_verm || 0}}</span></td>
    </tr>
  `).join('');

  /* ── gráfico comparativo aproveitamento recente vs geral ── */
  const chartData = formaSorted.slice(0, 15).map(f => {{
    const j  = f.jogos_recentes  || 0;
    const pt = f.pontos_recentes || 0;
    return {{
      time:       f.time,
      aprovRec:   j > 0 ? parseFloat(((pt / (j * 3)) * 100).toFixed(1)) : 0,
      aprovGeral: aprovGeralMap[f.time] || 0,
    }};
  }});

  new Chart(document.getElementById('chart-aprov-comparativo'), {{
    type: 'bar',
    data: {{
      labels: chartData.map(d => d.time),
      datasets: [
        {{
          label: 'Aproveitamento Recente',
          data: chartData.map(d => d.aprovRec),
          backgroundColor: 'rgba(245,197,24,0.75)',
          borderColor: '#F5C518',
          borderWidth: 1,
          borderRadius: 4,
        }},
        {{
          label: 'Aproveitamento Geral',
          data: chartData.map(d => d.aprovGeral),
          backgroundColor: 'rgba(0,155,58,0.5)',
          borderColor: '#009B3A',
          borderWidth: 1,
          borderRadius: 4,
        }},
      ]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ labels: {{ color: '#e6edf3' }} }},
      }},
      scales: {{
        x: {{ ticks: {{ color: '#8b949e', callback: v => v + '%' }}, grid: {{ color: '#30363d' }}, max: 100 }},
        y: {{ ticks: {{ color: '#e6edf3' }}, grid: {{ color: '#30363d' }} }},
      }},
    }}
  }});
}}

renderKPIs();
renderTabela();
</script>
</body>
</html>"""
    return html


def run():
    log.info("=" * 60)
    log.info("  Brasileirão 2026 — Gerando Dashboard")
    log.info("=" * 60)
    try:
        engine = get_engine()
        data = load_data(engine)
        html = gerar_html(data)
        output = os.path.join(BASE_DIR, "brasileirao_dashboard.html")
        with open(output, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"✔ Dashboard gerado: {output}")
        return output
    except Exception as e:
        log.error(f"✘ Erro: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run()
