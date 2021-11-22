SELECT DISTINCT
latitude as Latitude,
longitude as Longtitude,
city as City,
ZipCode,
Borough,
state as STATE
from {{ref('weather')}}