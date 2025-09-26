
--should we rebuild every run?
{{ config(
    materialized='table'
) }}

--using date_spine function in dbt_utils package
WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2050-01-01' as date)"
    ) }}

),

dim_date AS (

    SELECT
        TO_CHAR(date_day, 'YYYY-MM-DD') AS date_key,
        date_day,
        EXTRACT(year FROM date_day) AS year,
        EXTRACT(month FROM date_day) AS month,
        EXTRACT(day FROM date_day) AS day,
        EXTRACT(quarter FROM date_day) AS quarter,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        CASE WHEN EXTRACT(dow FROM date_day) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,
        extract(week FROM date_day) as week_of_year,
        extract(dow FROM date_day) as day_of_week
    FROM date_spine

)

SELECT *
FROM dim_date