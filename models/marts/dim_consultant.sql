{{ config( 
    materialized = 'incremental', 
    unique_key = 'consultant_name',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        consultant_name,
        contractor_flag,
        CURRENT_TIMESTAMP() AS create_dt
    FROM {{ ref('stg_consultant') }}
),

final AS (

    SELECT
        --Key
        ROW_NUMBER() OVER (ORDER BY consultant_name) AS consultant_key,
        -- Natural key and attributes
        consultant_name,
        contractor_flag,
        create_dt
    FROM base
)

SELECT * FROM final

{% if is_incremental() %}
WHERE consultant_name NOT IN (SELECT consultant_name FROM {{ this }})
{% endif %}
