
  
    

  create  table "brasileirao_pipeline"."public_marts"."mart_brasileirao__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_brasileirao.sql
-- Eh o modelo analítico final: une classificação, artilharia e performance em uma visão completa.
-- Materializado como tabela, usado p consultas performáticas em BI/dashboards.

with team_stats as (
    select * from "brasileirao_pipeline"."public_intermediate"."int_team_stats"
),

top_artilheiro_por_clube as (
    -- Aqui evidencia o artilheiro com mais gols de cada clube
    -- Uso DISTINCT ON do Postgres para pegar só o top 1 por clube
    select distinct on (clube)
        clube,
        jogador   as artilheiro_destaque,
        gols      as gols_artilheiro
    from "brasileirao_pipeline"."public_staging"."stg_artilharia"
    order by clube, gols desc
),

final as (
    select
        ts.posicao,
        ts.time,
        ts.pontos,
        ts.jogos,
        ts.vitorias,
        ts.empates,
        ts.derrotas,
        ts.gols_pro,
        ts.gols_contra,
        ts.saldo_gols,
        ts.aproveitamento,
        ts.media_gols_pro,
        ts.media_gols_contra,
        ts.performance_score,
        ts.jogos_casa,
        ts.vitorias_casa,
        ts.gols_pro_casa,
        ts.gols_contra_casa,
        ts.jogos_fora,
        ts.vitorias_fora,
        ts.gols_pro_fora,
        ts.gols_contra_fora,
        a.artilheiro_destaque,
        a.gols_artilheiro,

        -- Zonas da tabela
        case
            when ts.posicao <= 4  then 'Libertadores (G4)'
            when ts.posicao <= 6  then 'Sul-Americana (G6)'
            when ts.posicao >= 17 then 'Rebaixamento (Z4)'
            else 'Meio da tabela'
        end as zona,

        ts.updated_at

    from team_stats ts
    -- Join pelo nome do time vs nome do clube na artilharia
    left join top_artilheiro_por_clube a
        on lower(trim(ts.time)) = lower(trim(a.clube))

    order by ts.posicao
)

select * from final
  );
  