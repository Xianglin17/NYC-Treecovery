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
            DATE_ID as Closed_Date

          FROM (select DISTINCT
                Closed_Year as Year, 
                Closed_Month as Month, 
                Closed_Day as Day,
                Closed_Date as DATE_ID,
                from {{ref('311_complaints')}}
          ) 
    INNER JOIN {{ref('all_date')}} USING (DATE_ID)

),
total_table as (
    select Created_Year as Year, 
            Created_Month as Month, 
            Created_Day as Day,
            Created_Date as Date_ID,
            Closed_Date, 
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
            where Created_Year > 2016
)
SELECT Complaint_Type_Dim_ID, 
       Agency_Dim_ID, 
       Created_Date_Dim_ID,
       Closed_Date_Dim_ID,
       Location_Dim_ID,
       count(Final_ID) as Number_of_Complaint,
       ABS(date_diff(DATE(Closed_Date),DATE(Date_ID),day)) as Date_Gap

FROM (
SELECT Complaint_Type_Dim_ID, 
       Agency_Dim_ID, 
       Date_Dim_ID as Created_Date_Dim_ID,
       Closed_Date_Dim_ID,
       Location_Dim_ID,
       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Final_ID,
       Closed_Date,
       Date_ID


from total_table 
INNER JOIN complaint_table USING (complaint_type,status)
INNER JOIN agency_table USING (agency,agency_name)
INNER JOIN date_table USING (Year, Month, Day, Date_ID)
INNER JOIN location_table USING (Latitude,Longtitude,City,ZipCode,Borough,STATE)
INNER JOIN closed_date_table USING (Closed_Date)
)

Group by Complaint_Type_Dim_ID, Agency_Dim_ID, Created_Date_Dim_ID, Location_Dim_ID, Closed_Date_Dim_ID, Closed_Date, Date_ID
