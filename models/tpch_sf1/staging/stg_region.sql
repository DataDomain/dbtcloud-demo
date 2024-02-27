with 

source as (

    select * from {{ source('tpch_sf1', 'region') }}

),

renamed as (

    select
        r_regionkey     region_id,
        r_name          region_name,
        r_comment       region_comment,
        CONVERT_TIMEZONE('{{ var('DW_TIMEZONE') }}', CURRENT_TIMESTAMP()) DW_PROCESS_DT

    from source

)
 
select * from renamed
