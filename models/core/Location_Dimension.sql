with total as(
select * 
from {{ref('stg_311_location')}}
Union All
select * 
from {{ref('stg_weather_location')}}
)

SELECT DISTINCT *,ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS location_dim_ID
from total
