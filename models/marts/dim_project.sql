{{ config( 
    materialized = 'incremental', 
    unique_key = 'project_no',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        project_name,
        project_no,
        client_name,
        project_manager,
        project_category,
        project_description,
        project_start_date,
        project_due_date,
        project_client_order_no,
        project_milestones,
        project_milestones_completed,
        CURRENT_TIMESTAMP() AS create_dt
    FROM {{ ref('stg_project') }}

),

final AS (

    SELECT
        --Key
        ROW_NUMBER() OVER (ORDER BY project_no) AS project_key,
        c.client_key,
        -- Natural key and attributes
        project_no,
        project_name,
        project_manager,
        project_category,
        project_description,
        project_start_date, --consider FK to date
        project_due_date, --consider FK to date
        project_client_order_no,
        project_milestones,
        project_milestones_completed,
        base.create_dt
    FROM base
    LEFT JOIN {{ ref("dim_client") }} c
        ON UPPER(base.client_name) = UPPER(c.client_name)

)

SELECT * FROM final

{% if is_incremental() %}
WHERE project_no NOT IN (SELECT project_no FROM {{ this }})
{% endif %}