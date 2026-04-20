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

    {% if is_incremental() %}
    WHERE client_name NOT IN (
        SELECT client_name FROM {{ this }}
    )
    {% endif %}
),

unknown as (
    select
        {{ dbt_utils.generate_surrogate_key(['-1']) }} as client_key,
        'Unknown' as client_name,
        '1900-01-01' AS created_dt
),

final AS (

    SELECT
        --Key
        {{ dbt_utils.generate_surrogate_key(['client_name']) }} as client_key,
        -- Natural key and attributes
        client_name,
        create_dt
    FROM base
)

SELECT * FROM final
union
select * from unknown

