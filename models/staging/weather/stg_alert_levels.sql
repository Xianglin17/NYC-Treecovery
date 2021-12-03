with levels as (select DISTINCT
level, 
Weather_Type
from {{ref('weather')}})

select *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Alert_Levels_Dim_ID
from levels

