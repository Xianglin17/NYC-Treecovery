with all_date as (select * from {{ref('stg_complaints_date')}}
Union ALL
select * from {{ref('stg_weather_date')}}
)

select *,ROW_NUMBER() OVER (ORDER BY (SELECT Date_ID)) AS Date_dim_ID
FROM(
select DISTINCT *
from all_date)