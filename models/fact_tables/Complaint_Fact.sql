with complaint as (
    select Complaint_Type_Dim_ID from {{ref('Complaint_Dimension')}}
),
agency as (
    select Agency_Dim_ID from {{ref('Agency_Dimension')}}
),
datetable as (
    select Date_Dim_ID from {{ref('Date_Dimension')}}
), 
location as (
    select location_dim_ID as Location_Dim_ID from {{ref('Location_Dimension')}}
)
select * from complaint, agency, datetable, location