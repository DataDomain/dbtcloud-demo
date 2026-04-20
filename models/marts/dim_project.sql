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

    {% if is_incremental() %}
    WHERE project_no NOT IN (
            SELECT project_no FROM {{ this }}
    )
    {% endif %}

),

unknown as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['-1']) }} as project_key,
        'Unknown' as client_name,
        'Unknown' as project_no,
        'Unknown' as project_name,
        'Unknown' as project_manager,
        'Unknown' as project_category,
        'Unknown' as project_description,
        '1900-01-01' as project_start_date,
        '1900-01-01' as project_due_date,
        'Unknown' project_client_order_no,
        'Unknown' as project_milestones,
        'Unknown' as project_milestones_completed,
        '1900-01-01' AS create_dt

),

unknown_join as (
    select
        u.project_key,
        c.client_key,
        u.project_no,
        u.project_name,
        u.project_manager,
        u.project_category,
        u.project_description,
        u.project_start_date,
        u.project_due_date,
        u.project_client_order_no,
        u.project_milestones,
        u.project_milestones_completed,
        u.create_dt
    from unknown u
    left join {{ ref("dim_client") }} c
        ON UPPER(u.client_name) = UPPER(c.client_name)

),

final AS (
    SELECT
        --Key
        {{ dbt_utils.generate_surrogate_key(['project_no']) }} as project_key,
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
union 
select * from unknown_join

