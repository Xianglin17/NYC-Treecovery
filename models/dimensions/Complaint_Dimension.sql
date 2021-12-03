{{ config (
    materialized="table"
)}}

SELECT * 
from {{ref('stg_complaint')}}