select 
Created_Year as Year,
Created_Month as Month, 
Created_Day as Day,
Created_Date as Date_ID
from{{ref('311_complaints')}}
Union all
select 
Closed_Year as Year,
Closed_Month as Month, 
Closed_Day as Day,
Closed_Date as Date_ID
from{{ref('311_complaints')}}