with 

source as (

    select * from {{ source('tpch_sf1', 'supplier') }}

),

renamed as (

    select
        s_suppkey       supplier_id,
        s_name          supplier_name,
        s_address       supplier_address,
        s_nationkey     supplier_country_id,
        s_phone         supplier_phone,
        s_acctbal       supplier_account_balance,
        s_comment       supplier_comment

    from source

)

select * from renamed
