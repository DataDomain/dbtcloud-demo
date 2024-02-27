{{ config(
    materialized= "table"
)}}

with source as (

    select * from {{ source('tpch_sf1', 'customer') }}

),

renamed as (

    select
        c_custkey       customer_id,
        c_name          customer_name,
        c_address       customer_address,
        c_nationkey     customer_country_code,
        c_phone         customer_phone,
        c_acctbal       customer_account_balance,
        c_mktsegment    customer_mkt_segment,
        c_comment       customer_comment,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT

    from source

)

select * from renamed 