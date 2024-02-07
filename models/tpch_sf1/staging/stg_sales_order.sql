with 

source as (

    select * from {{ source('tpch_sf1', 'orders') }}

),

renamed as (

    select
        o_orderkey          order_id,
        o_custkey           customer_id,
        o_orderstatus       order_status,
        o_totalprice        total_price,
        o_orderdate         order_date,
        o_orderpriority     order_priority,
        o_clerk             order_clerk,
        o_shippriority      order_ship_priority,
        o_comment           order_comment,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT

    from source

)

select * from renamed
