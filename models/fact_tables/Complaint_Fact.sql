with complaint as (
    select * from {{ref('Complaint_Dimension')}}
),
agency as (
    select * from {{ref('Agency_Dimension')}} 
),
datetable as (
    select *
    from {{ref('Date_Dimension')}} 
),
location as (
    select *
    from {{ref('Location_Dimension')}} 
),
total as (
    select Created_Year as Year, 
            Created_Month as Month, 
            Created_Day as Day,
            Date_ID,
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

select Complaint_Type_Dim_ID,Agency_Dim_ID,Date_Dim_ID,Location_Dim_ID
from Total 
LEFT JOIN complaintUSING (complaint_type,status)
LEFT JOIN agency USING (agency,agency_name)
LEFT JOIN datetable USING (Year, Month, Day, Date_ID)
LEFT JOIN location USING (Latitude, Longtitude, City, ZipCode,Borough, STATE)
