{{
    config(
        materialized='incremental',
        unique_key='part_key'
    )
}}

with stg as (
    select {{ dbt_utils.star(ref('stg_part')) }}
    from {{ ref('stg_part') }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['PART_ID']) }} "PART_KEY",
    "PART_ID",
    "PART_NAME",
    "PART_MANFACTURER",
    "PART_BRAND",
    "PART_TYPE",
    "PART_SIZE",
    "PART_CONTAINER",
    "PART_PRICE",
    "PART_COMMENT",
    CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT
from stg
