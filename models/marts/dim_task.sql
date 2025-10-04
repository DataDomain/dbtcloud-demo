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

final AS (

    SELECT
        --Key
        ROW_NUMBER() OVER (ORDER BY base.project_no, task_label, task_category) AS task_key,
        p.project_key,
        -- Natural key and attributes
        base.project_no,
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