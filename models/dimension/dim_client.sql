{{ config( 
    materialized = 'incremental', 
    unique_key = 'client_name',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        client_name,
        CURRENT_TIMESTAMP() AS create_dt
    FROM {{ ref('stg_client') }}
),

final AS (

    SELECT
        --Key
        ROW_NUMBER() OVER (ORDER BY client_name) AS client_key,
        -- Natural key and attributes
        client_name,
        create_dt
    FROM base
)

SELECT * FROM final

{% if is_incremental() %}
WHERE client_name NOT IN (SELECT client_name FROM {{ this }})
{% endif %}