with all_date as (select * from {{ref('stg_complaints_date')}}
Union ALL
select * from {{ref('stg_weather_date')}})

select DISTINCT *,ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Date_dim_ID
from all_date