-- models/staging/stg_classificacao.sql
-- Limpeza e tipagem da tabela de classificação carregada pelo load_database.py

with source as (
    select * from {{ source('brasileirao_raw', 'tabela') }}
),

renamed as (
    select
        posicao::integer                        as posicao,
        trim(time)                              as time,
        pontos::integer                         as pontos,
        jogos::integer                          as jogos,
        vitorias::integer                       as vitorias,
        empates::integer                        as empates,
        derrotas::integer                       as derrotas,
        gols_pro::integer                       as gols_pro,
        gols_contra::integer                    as gols_contra,
        saldo_gols::integer                     as saldo_gols,
        round(aproveitamento::numeric, 2)       as aproveitamento,
        current_timestamp                       as updated_at
    from source
)

select * from renamed