{{
    config(
        materialized='incremental',
        unique_key='supplier_key'
    )
}}

with stg_supplier as (
    select {{ dbt_utils.star(ref('stg_supplier')) }}
    from {{ ref('stg_supplier') }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['SUPPLIER_ID']) }} "SUPPLIER_KEY",
    "SUPPLIER_ID",
    "SUPPLIER_NAME",
    "SUPPLIER_ADDRESS",
    "SUPPLIER_COUNTRY_ID",
    "SUPPLIER_PHONE",
    "SUPPLIER_ACCOUNT_BALANCE",
    CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT
from stg_supplier
