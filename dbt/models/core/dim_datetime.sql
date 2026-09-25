{{ config(materialized='table')}}

WITH date_series AS (
    SELECT generate_series(
        TIMESTAMP '2020-01-01 00:00:00',
        TIMESTAMP '2026-12-31 23:00:00',
        INTERVAL '1 hour'
    ) AS date
)

SELECT  
    EXTRACT(EPOCH FROM date)::bigint AS dateKey,
    date,
    EXTRACT(DOW FROM date) + 1 AS dayOfWeek,
    EXTRACT(DAY FROM date) AS dayOfMonth,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(WEEK FROM date) AS weekOfYear,   
    EXTRACT(YEAR FROM date) AS year,
    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN true ELSE false END AS weekendFlag
FROM date_series
