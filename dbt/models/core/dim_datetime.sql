{{ config(materialized='table')}}

WITH date_series AS (
    SELECT *
    FROM UNNEST(
        GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('2020-01-01'), TIMESTAMP('2025-12-31'), INTERVAL 1 HOUR)
         ) AS date
)

SELECT  
    UNIX_SECONDS(date) AS dateKey,
    date,
    EXTRACT(DAYOFWEEK FROM date) AS DayOfWeek,
    EXTRACT(DAY FROM date) AS dayOfMonth,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(WEEK FROM date) AS weekOfYear,   
    EXTRACT(YEAR FROM date) AS year,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN(6,7) THEN True ESLE FALSE AS weekendFlag
FROM date_series

