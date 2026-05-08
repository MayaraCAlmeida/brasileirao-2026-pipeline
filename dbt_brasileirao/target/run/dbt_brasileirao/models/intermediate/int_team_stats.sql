
  create view "brasileirao_pipeline"."public_intermediate"."int_team_stats__dbt_tmp"
    
    
  as (
    -- models/intermediate/int_team_stats.sql
-- Agrega métricas de performance por time, usando as colunas reais do banco

with classificacao as (
    select * from "brasileirao_pipeline"."public_staging"."stg_classificacao"
),

team_stats_raw as (
    select * from "brasileirao_pipeline"."public"."team_stats"
),

joined as (
    select
        c.posicao,
        c.time,
        c.pontos,
        c.jogos,
        c.vitorias,
        c.empates,
        c.derrotas,
        c.gols_pro,
        c.gols_contra,
        c.saldo_gols,
        c.aproveitamento,

        -- Métricas por jogo calculadas aqui
        round(c.gols_pro::numeric / nullif(c.jogos, 0), 2)     as media_gols_pro,
        round(c.gols_contra::numeric / nullif(c.jogos, 0), 2)  as media_gols_contra,

        -- Performance score (mesmo cálculo do transform_data.py, agora em SQL)
        round(
            (c.aproveitamento * 0.5)
            + (c.saldo_gols::numeric / nullif(c.jogos, 0) * 0.3)
            + (c.gols_pro::numeric / nullif(c.jogos, 0) * 0.2),
            2
        )                                                        as performance_score,

        -- Stats casa/fora vindas direto do team_stats
        ts.jogos_casa,
        ts.vitorias_casa,
        ts.empates_casa,
        ts.derrotas_casa,
        ts.gols_pro_casa,
        ts.gols_contra_casa,
        ts.jogos_fora,
        ts.vitorias_fora,
        ts.empates_fora,
        ts.derrotas_fora,
        ts.gols_pro_fora,
        ts.gols_contra_fora,

        c.updated_at

    from classificacao c
    left join team_stats_raw ts
        on lower(trim(c.time)) = lower(trim(ts.time))
)

select * from joined
  );