{{ config(
    materialized='table'
) }}

WITH timesheet as (
    SELECT *
    FROM {{ ref('stg_datadomain_timesheet') }}
),

dim_date as (
    SELECT *
    FROM {{ ref('dim_date') }} 
),

dim_consultant as (
    SELECT *
    FROM {{ ref('dim_consultant') }} 
),

dim_client as (
    SELECT *
    FROM {{ ref('dim_client') }} 
),

-- dim_project as (
--     SELECT *
--     FROM {{ ref('dim_project') }} 
-- ),

-- dim_task as (
--     SELECT *
--     FROM {{ ref('dim_task') }} 
-- ),

final as (
    SELECT
        d_d.date_key,
        d_co.consultant_key,
        d_cl.client_key,
        ts.actual_time,
        ts.billable,
        ts.category
    FROM timesheet as ts
    LEFT JOIN dim_date as d_d on ts.date = d_d.date_day
    LEFT JOIN dim_consultant as d_co on ts.staff_name = d_co.consultant_name
    LEFT JOIN dim_client as d_cl on ts.client = d_cl.client_name
    --LEFT JOIN dim_project as d_p on 
)

select * from final