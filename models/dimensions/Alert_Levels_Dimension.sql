select *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Alert_Levels_Dim_ID
from {{ref('stg_alert_levels')}}