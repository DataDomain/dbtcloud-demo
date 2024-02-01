with 

source as (

    select * from {{ source('tpch_sf1', 'region') }}

),

renamed as (

    select
        r_regionkey     region_id,
        r_name          region_name,
        r_comment       region_comment

    from source

)

select * from renamed
