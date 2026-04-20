{{ config( 
    materialized = 'incremental', 
    unique_key = ['project_no','task_label','task_category'],
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        project_no,
        task_label,
        task_category,
        task_billable,
        task_remaining_time,
        task_actual_time,
        task_estimated_time,
        CURRENT_TIMESTAMP() AS create_dt
    FROM {{ ref('stg_task') }}
    
),

unknown as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['-1']) }} as task_key,
        'Unknown' as project_no,
        'Unknown' as task_label,
        'Unknown' as task_category,
        null as task_billable,
        null as task_remaining_time,
        null as task_actual_time,
        null as task_estimated_time,
        '1900-01-01' as create_dt
),

unknown_join as (
    SELECT
        u.task_key,
        p.project_key,
        u.task_label,
        u.task_category,
        u.task_billable,
        u.task_remaining_time,
        u.task_actual_time,
        u.task_estimated_time,
        u.create_dt
    from unknown u 
    LEFT JOIN {{ ref("dim_project") }} p
        ON UPPER(u.project_no) = UPPER(p.project_no)

),

final AS (

    SELECT
        --Key
        {{ dbt_utils.generate_surrogate_key(['task_label','task_category','p.project_key']) }} as task_key,
        p.project_key,
        -- Natural key and attributes
        task_label,
        task_category,
        task_billable,
        task_remaining_time,
        task_actual_time,
        task_estimated_time,
        base.create_dt
    FROM base
    LEFT JOIN {{ ref("dim_project") }} p
        ON UPPER(base.project_no) = UPPER(p.project_no)

),

new_record AS (

    SELECT f.*
    FROM final f

    {% if is_incremental() %}
    LEFT JOIN {{ this }} t
        ON f.project_no = t.project_no
        AND f.task_label = t.task_label
        AND f.task_category = t.task_category
    WHERE 
        -- new records
        t.project_no IS NULL 
        -- updated records

    {% endif %}

)

select * from new_record
union
select * from unknown_join