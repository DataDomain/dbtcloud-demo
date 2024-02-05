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
{% if is_incremental() %}
, lkp_dim as (
    select 
        "CUSTOMER_KEY",                     -- artificial/ primary key
        "CUSTOMER_ID",                      -- business key
        "CUSTOMER_NAME",
        "CUSTOMER_ADDRESS",                 -- change tracking columns
        "CUSTOMER_COUNTRY_CODE",
        "CUSTOMER_PHONE",
        "CUSTOMER_ACCOUNT_BALANCE",
        "CUSTOMER_MKT_SEGMENT",
        DW_PROCESS_DT,    
        DW_VALID_FROM,
        DW_VALID_TO,
        DW_CURRENT_FLAG
    from 
        {{ this }} 
    where 
        DW_CURRENT_FLAG = 'Y'
)
-- expiring rows
    select 
        dim."CUSTOMER_KEY",
        dim."CUSTOMER_ID",
        dim."CUSTOMER_NAME",
        dim."CUSTOMER_ADDRESS",
        dim."CUSTOMER_COUNTRY_CODE",
        dim."CUSTOMER_PHONE",               -- candidate Type 1 column:stg."CUSTOMER_PHONE"
        dim."CUSTOMER_ACCOUNT_BALANCE",
        dim."CUSTOMER_MKT_SEGMENT",
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT,
        dim.DW_VALID_FROM,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', TIMESTAMPADD( second, -1, CURRENT_TIMESTAMP())) DW_VALID_TO,
        'N' DW_CURRENT_FLAG
    from 
        stg_customer stg
    left join lkp_dim dim
        on stg.customer_id = dim.customer_id
    where 
        dim.customer_id IS NOT NULL
        and dim.CUSTOMER_ADDRESS <> stg.CUSTOMER_ADDRESS
    UNION    
-- new inserts
    select 
        {{ dbt_utils.generate_surrogate_key(['stg.CUSTOMER_ID','stg.CUSTOMER_ADDRESS']) }} "CUSTOMER_KEY",
        stg."CUSTOMER_ID",
        stg."CUSTOMER_NAME",
        stg."CUSTOMER_ADDRESS",
        stg."CUSTOMER_COUNTRY_CODE",
        stg."CUSTOMER_PHONE",
        stg."CUSTOMER_ACCOUNT_BALANCE",
        stg."CUSTOMER_MKT_SEGMENT",
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_VALID_FROM,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', TO_TIMESTAMP('31/12/2050','DD/MM/YYYY')) DW_VALID_TO,
        'Y' DW_CURRENT_FLAG
    from 
        stg_customer stg
    left join lkp_dim dim
        on stg.customer_id = dim.customer_id
    where 
        dim.customer_id IS NOT NULL
        OR  
        (dim.customer_id IS NOT NULL
        and dim.CUSTOMER_ADDRESS <> stg.CUSTOMER_ADDRESS)
{% endif %}
{% if is_incremental() == False %}
    select 
        {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID','CUSTOMER_ADDRESS']) }} "CUSTOMER_KEY",
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
        "CUSTOMER_ADDRESS",
        "CUSTOMER_COUNTRY_CODE",
        "CUSTOMER_PHONE",
        "CUSTOMER_ACCOUNT_BALANCE",
        "CUSTOMER_MKT_SEGMENT",
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_VALID_FROM,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', TO_TIMESTAMP('31/12/2050','DD/MM/YYYY')) DW_VALID_TO,
        'Y' DW_CURRENT_FLAG
    from 
        stg_customer stg
{% endif %}
