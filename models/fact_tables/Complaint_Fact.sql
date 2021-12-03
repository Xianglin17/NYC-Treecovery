{{ config (
    materialized="table"
)}}

with complaint_table as (
    select * from {{ref('Complaint_Dimension')}}
),
agency_table as (
    select * from {{ref('Agency_Dimension')}} 
),
date_table as (
    select * from {{ref('Date_Dimension')}} 
),
location_table as (
    select *
    from {{ref('Location_Dimension')}} 
),
closed_date_table as (
    select  Date_Dim_ID as Closed_Date_Dim_ID,
            Date_ID as Closed_Date  
    from(select      
            Closed_Year as Year, 
            Closed_Month as Month, 
            Closed_Day as Day,
            Closed_Date as Date_ID 
          from {{ref('311_complaints')}}
          ) 
    Left Join 
        (select * from {{ref('Date_Dimension')}} ) USING (Year, Month, Day, Date_ID)
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
SELECT Complaint_Type_Dim_ID, 
       Agency_Dim_ID, 
       Created_Date_Dim_ID,
       Closed_Date_Dim_ID,
       Location_Dim_ID,
       count(*) as Number_of_Complaint,
       ABS(date_diff(DATE(Closed_Date),DATE(Date_ID),day)) as Date_Gap

FROM (
SELECT Complaint_Type_Dim_ID, 
       Agency_Dim_ID, 
       Date_Dim_ID as Created_Date_Dim_ID,
       Closed_Date_Dim_ID,
       Location_Dim_ID,
       Closed_Date,
       Date_ID


from (total_table 
LEFT JOIN complaint_table USING (complaint_type,status)
LEFT JOIN agency_table USING (agency,agency_name)
LEFT JOIN date_table USING (Year, Month, Day, Date_ID)
LEFT JOIN location_table USING (Latitude,Longtitude,City,ZipCode,Borough,STATE)),
closed_date_table
)

Group by Complaint_Type_Dim_ID, Agency_Dim_ID, Created_Date_Dim_ID, Location_Dim_ID, Closed_Date_Dim_ID, Closed_Date, Date_ID
