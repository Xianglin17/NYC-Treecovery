{{config (
    materialized="table"
)}}

WITH DATE_SPINE as (
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2010-01-01' as date)",
    end_date="cast('2021-01-01' as date)"
   )
}} )

SELECT
ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as Date_ID,
(CAST(DATE_DAY AS DATE)) AS DATE_KEY
FROM DATE_SPINE