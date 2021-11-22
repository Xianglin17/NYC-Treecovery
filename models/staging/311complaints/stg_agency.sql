SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS agencyID, agency, agency_name
FROM (SELECT DISTINCT agency, agency_name
FROM {{ref('311_complaints')}})

