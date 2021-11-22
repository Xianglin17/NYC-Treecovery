with alert_levels_table as (
    select * from {{ref('Alert_Levels_Dimension')}}
),
datetable_table as (
    select *
    from {{ref('Date_Dimension')}} 
),
location_table as (
    select *
    from {{ref('Location_Dimension')}} 
),
total_table as (
    select  Year, 
            Month, 
            Day,
            Date as Date_ID,
            latitude as Latitude,
            longitude as Longtitude,
            City,
            ZipCode,
            Borough,
            STATE,
            level, 
            Weather_Type,
            Precipitation_Total,
            Temperature_Max,
            Temperature_Avg,
            Temperature_M,
            Wdspeed_Avg,
            Wdspeed_Max,
            Wdspeed_M,
            from {{ref('weather')}} 
)

select Alert_Levels_Dim_ID ,
       Date_Dim_ID,
       Location_Dim_ID, 
       Precipitation_Total,
       Temperature_Max,
       Temperature_Avg,
       Temperature_M as Temperature_Min,
       Wdspeed_Avg as Average_Wind_Speed,
       Wdspeed_Max as Max_Wind_Speed,
       Wdspeed_M as Min_Wind_Speed
from total_table 
LEFT JOIN alert_levels_table USING (level, Weather_Type)
LEFT JOIN datetable_table USING (Year, Month, Day, Date_ID)
LEFT JOIN location_table USING (Latitude,Longtitude,City,ZipCode,Borough,STATE)
Order by Date_Dim_ID

