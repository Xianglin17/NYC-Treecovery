SELECT
ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Complaint_Type_Dim_ID,
complaint_type,
status
from (SELECT DISTINCT complaint_type,status
FROM {{ref('311_complaints')}})


