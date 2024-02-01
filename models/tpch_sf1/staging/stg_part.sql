with 

source as (

    select * from {{ source('tpch_sf1', 'part') }}

),

renamed as (

    select
        p_partkey       part_id,
        p_name          part_name,
        p_mfgr          part_manfacturer,
        p_brand         part_brand,
        p_type          part_type,
        p_size          part_size,
        p_container     part_container,
        p_retailprice   part_price,
        p_comment       part_comment

    from source

)

select * from renamed
