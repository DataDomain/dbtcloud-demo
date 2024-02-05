{{ config(materialized="incremental", unique_key="order_item_key") }}

with
    stg_item as (
        select {{ dbt_utils.star(ref("stg_sales_order_item")) }}
        from {{ ref("stg_sales_order_item") }}
    ),
    stg as (
        select {{ dbt_utils.star(ref("stg_sales_order")) }}
        from {{ ref("stg_sales_order") }}
    ),
    dim_customer as (select customer_key, customer_id from {{ ref("dim_customer") }}),
    dim_supplier as (select supplier_key, supplier_id from {{ ref("dim_supplier") }}),
    dim_part as (select part_key, part_id from {{ ref("dim_part") }})
select
    {{ dbt_utils.generate_surrogate_key(['ORDER_ID', 'ITEM_NUM']) }} order_item_key,
    "ORDER_ID",
    -- "PART_ID",
    -- "SUPPLIER_ID",
    "ITEM_NUM",
    "ITEM_QUANTITY",
    "EXTENDED_PRICE",
    "ITEM_DISCOUNT",
    "ITEM_TAX",
    "ITEM_RETURN_FLAG",
    "ITEM_STATUS",
    "ITEM_SHIP_DATE",
    "ITEM_COMMIT_DATE",
    "ITEM_RECEIPT_DATE",
    "ITEM_SHIP_INSTRUCTIONS",
    "ITEM_SHIP_MODE",
    -- "CUSTOMER_ID",
    "ORDER_STATUS",
    "TOTAL_PRICE",
    "ORDER_DATE",
    "ORDER_PRIORITY",
    "ORDER_CLERK",
    "ORDER_SHIP_PRIORITY",
    "ORDER_COMMENT",
    "ITEM_COMMENT",
    dim_part.part_key,
    dim_customer.customer_key,
    dim_supplier.supplier_key
from stg
left join stg_item on stg.order_id = stg_item.item_order_id
left join dim_customer on stg.customer_id = dim_customer.customer_id
left join dim_part on stg_item.part_id = dim_part.part_id
left join dim_supplier on stg_item.supplier_id = dim_supplier.supplier_id
