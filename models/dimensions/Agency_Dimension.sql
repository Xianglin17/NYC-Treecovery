{{ config (
    materialized="table"
)}}

select *
FROM {{ref('stg_agency')}}
