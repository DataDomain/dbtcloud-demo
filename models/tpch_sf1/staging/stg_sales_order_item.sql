with 

source as (

    select * from {{ source('tpch_sf1', 'lineitem') }}

),

renamed as (

    select
        l_orderkey      item_order_id,
        l_partkey       part_id,
        l_suppkey       supplier_id,
        l_linenumber    item_num,
        l_quantity      item_quantity,
        l_extendedprice extended_price ,
        l_discount      item_discount,
        l_tax           item_tax,
        l_returnflag    item_return_flag,
        l_linestatus    item_status,
        l_shipdate      item_ship_date,
        l_commitdate    item_commit_date,
        l_receiptdate   item_receipt_date,
        l_shipinstruct  item_ship_instructions,
        l_shipmode      item_ship_mode,
        l_comment       item_comment

    from source

)

select * from renamed
