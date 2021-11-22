SELECT DISTINCT
latitude as Latitude,
longitude as Longtitude,
city as City,
incident_zip as ZipCode,
borough as Borough,
state as STATE
from {{ref('311_complaints')}}