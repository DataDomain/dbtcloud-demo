{{
    config(
        materialized='incremental',
        unique_key='customer_key'
    )
}}

with stg_customer as (
    select {{ dbt_utils.star(ref('stg_customer')) }}
    from {{ ref('stg_customer') }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} "CUSTOMER_KEY",
    "CUSTOMER_ID",
    "CUSTOMER_NAME",
    "CUSTOMER_ADDRESS",
    "CUSTOMER_COUNTRY_CODE",
    "CUSTOMER_PHONE",
    "CUSTOMER_ACCOUNT_BALANCE",
    "CUSTOMER_MKT_SEGMENT",
    CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT
from stg_customer