{{ config( 
    materialized = 'incremental', 
    unique_key = 'consultant_name'
) }}

WITH base AS (

    SELECT
        consultant_name,
        contractor_flag
    FROM {{ ref('stg_consultant') }}
),

final AS (

    SELECT
        --Key
        ROW_NUMBER() OVER (ORDER BY consultant_name) AS consultant_key,
        -- Natural key and attributes
        consultant_name,
        contractor_flag
    FROM base
)

SELECT * FROM final
