--- 1. Tabela de classificação completa
SELECT
    posicao,
    time,
    pontos,
    jogos,
    vitorias,
    empates,
    derrotas,
    gols_pro,
    gols_contra,
    saldo_gols,
    ROUND(aproveitamento::numeric, 1) AS aproveitamento_pct
FROM tabela
ORDER BY posicao;


--- 2. Top 10 artilheiros
SELECT
    jogador,
    clube,
    gols
FROM artilharia
ORDER BY gols DESC
LIMIT 10;


--- 3. Melhor ataque e melhor defesa
SELECT
    time,
    total_jogos,
    total_gols_pro,
    ROUND(media_gols_pro::numeric, 2)      AS media_gols_pro,
    total_gols_contra,
    ROUND(media_gols_contra::numeric, 2)   AS media_gols_contra,
    saldo_gols_calc,
    ROUND(performance_score::numeric, 1)   AS performance_score
FROM team_stats
ORDER BY media_gols_pro DESC;


--- 4. Home vs Away — quem manda em casa?
SELECT
    time,
    jogos_casa,
    vitorias_casa,
    ROUND(100.0 * vitorias_casa / NULLIF(jogos_casa, 0), 1) AS win_pct_casa,
    jogos_fora,
    vitorias_fora,
    ROUND(100.0 * vitorias_fora / NULLIF(jogos_fora, 0), 1) AS win_pct_fora,
    (vitorias_casa - vitorias_fora)                          AS diff_vitorias
FROM team_stats
ORDER BY win_pct_casa DESC;


--- 5. Forma recente — últimas 5 rodadas
SELECT
    t.posicao,
    f.time,
    f.forma_str,
    f.pontos_recentes,
    f.vitorias_recentes,
    f.empates_recentes,
    f.derrotas_recentes,
    ROUND(f.aproveitamento_recente::numeric, 1) AS aproveitamento_recente
FROM forma_recente f
JOIN tabela t ON f.time = t.time
ORDER BY f.aproveitamento_recente DESC;


--- 6. Partidas com mais gols (top 10)
SELECT
    rodada,
    mandante,
    gols_mandante,
    gols_visitante,
    visitante,
    total_gols,
    estadio
FROM partidas_finalizadas
ORDER BY total_gols DESC, rodada DESC
LIMIT 10;


--- 7. Tendência de gols por rodada
SELECT
    rodada,
    jogos,
    total_gols,
    ROUND(media_gols::numeric, 2) AS media_gols_por_jogo
FROM gols_por_rodada
ORDER BY rodada;


--- 8. Times em sequência positiva (3+ jogos sem perder)
WITH ultimos_jogos AS (
    SELECT mandante AS time, rodada, resultado_mandante AS resultado
    FROM partidas_finalizadas
    UNION ALL
    SELECT visitante, rodada, resultado_visitante
    FROM partidas_finalizadas
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY time ORDER BY rodada DESC) AS rn
    FROM ultimos_jogos
)
SELECT
    time,
    COUNT(*)                                          AS jogos_sem_perder,-- 
    STRING_AGG(resultado, '' ORDER BY rn DESC)        AS sequencia
FROM ranked
WHERE resultado IN ('V', 'E') AND rn <= 5
GROUP BY time
HAVING COUNT(*) >= 3
ORDER BY jogos_sem_perder DESC;


--- 9. Comparação direta entre dois times 
--- À medida que o campeonato avança e mais confrontos acontecem, é só trocar os nomes.
SELECT
    rodada,
    mandante,
    gols_mandante,
    gols_visitante,
    visitante,
    estadio,
    resultado_mandante
FROM partidas_finalizadas
WHERE
    (mandante = 'São Paulo' AND visitante = 'Flamengo')
 OR (mandante = 'Flamengo' AND visitante = 'São Paulo')
ORDER BY rodada;


--- 10. Ranking de performance geral
SELECT
    t.posicao,
    ts.time,
    t.pontos,
    ROUND(ts.performance_score::numeric, 1)  AS performance_score,
    ROUND(ts.aproveitamento_calc::numeric, 1) AS aproveitamento_pct,
    ts.total_gols_pro,
    ts.saldo_gols_calc,
    f.forma_str
FROM team_stats ts
JOIN tabela t        ON ts.time = t.time
JOIN forma_recente f ON ts.time = f.time
ORDER BY ts.performance_score DESC;


---- corrigindo

SELECT time FROM tabela ORDER BY posicao;
SELECT time FROM forma_recente LIMIT 5;
SELECT time FROM team_stats LIMIT 5;


SELECT DISTINCT mandante FROM partidas ORDER BY mandante;

SELECT mandante, gols_mandante, gols_visitante, visitante, rodada
FROM partidas_finalizadas
ORDER BY rodada;