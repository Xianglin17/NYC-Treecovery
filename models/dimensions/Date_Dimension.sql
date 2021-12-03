{{ config (
    materialized="table"
)}}

with union_date as (select * from {{ref('stg_complaints_date')}}
Union ALL
select * from {{ref('stg_weather_date')}}
)

select *
FROM(
select DISTINCT *
from union_date)
Left Join {{ref('all_date')}} USING (DATE_ID)
