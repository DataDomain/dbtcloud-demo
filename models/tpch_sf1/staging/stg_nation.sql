with 

source as (

    select * from {{ source('tpch_sf1', 'nation') }}

),

renamed as (

    select
        n_nationkey     country_id,
        n_name          country_name,
        n_regionkey     country_region_id,
        n_comment       country_comment

    from source

)

select * from renamed
