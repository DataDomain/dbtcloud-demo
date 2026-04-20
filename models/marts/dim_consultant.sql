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

    {% if is_incremental() %}
    where consultant_name not in (
        select consultant_name from {{ this }}
    )
    {% endif %}
),

unknown as (
    select
        {{ dbt_utils.generate_surrogate_key(['-1']) }} as consultant_key,
        'Unknown' as consultant_name,
        null as contractor_flag,
        '1900-01-01' AS created_dt
),

final AS (

    SELECT
        --Key
        {{ dbt_utils.generate_surrogate_key(['consultant_name']) }} as consultant_key,
        -- Natural key and attributes
        consultant_name,
        contractor_flag,
        create_dt
    FROM base
)

SELECT * FROM final
UNION
select * from unknown
