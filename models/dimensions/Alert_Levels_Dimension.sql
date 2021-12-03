{{ config (
    materialized="table"
)}}

select *
from {{ref('stg_alert_levels')}}
