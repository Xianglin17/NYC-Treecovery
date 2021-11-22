select 
Year,
Month, 
Day,
Date as Date_ID
from{{ref('weather')}}