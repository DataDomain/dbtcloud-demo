with 

source as (

    select * from {{ source('tpch_sf1', 'partsupp') }}

),

renamed as (

    select
        ps_partkey      part_id,
        ps_suppkey      part_supplier_id,
        ps_availqty     part_available_quantity,
        ps_supplycost   part_supply_cost,
        ps_comment      part_supply_comment,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT

    from source

)

select * from renamed

