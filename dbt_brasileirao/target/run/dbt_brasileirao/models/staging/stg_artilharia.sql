
  create view "brasileirao_pipeline"."public_staging"."stg_artilharia__dbt_tmp"
    
    
  as (
    -- models/staging/stg_artilharia.sql
-- Limpeza e tipagem da tabela de artilharia
-- Aqui a coluna do time é "clube" nesta tabela (eh diferente das demais que usam "time")

with source as (
    select * from "brasileirao_pipeline"."public"."artilharia"
),

renamed as (
    select
        posicao::integer                    as posicao,
        trim(jogador)                       as jogador,
        trim(clube)                         as clube,
        gols::integer                       as gols,
        current_timestamp                   as updated_at
    from source
)

select * from renamed
  );