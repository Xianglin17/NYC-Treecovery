with complaint_table as (
    select * from {{ref('Complaint_Dimension')}}
),
agency_table as (
    select * from {{ref('Agency_Dimension')}} 
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
    select Created_Year as Year, 
            Created_Month as Month, 
            Created_Day as Day,
            Created_Date as Date_ID,
            latitude as Latitude,
            longitude as Longtitude,
            city as City,
            incident_zip as ZipCode,
            borough as Borough,
            state as STATE,
            agency, 
            agency_name,
            complaint_type,
            status
            from {{ref('311_complaints')}} 
)

select Complaint_Type_Dim_ID, Agency_Dim_ID, Date_Dim_ID,Location_Dim_ID, count(*) as Number_of_Complaint
from total_table 
LEFT JOIN complaint_table USING (complaint_type,status)
LEFT JOIN agency_table USING (agency,agency_name)
LEFT JOIN datetable_table USING (Year, Month, Day, Date_ID)
LEFT JOIN location_table USING (Latitude,Longtitude,City,ZipCode,Borough,STATE)
Group by Complaint_Type_Dim_ID, Agency_Dim_ID, Date_Dim_ID,Location_Dim_ID
order by Number_of_Complaint DESC