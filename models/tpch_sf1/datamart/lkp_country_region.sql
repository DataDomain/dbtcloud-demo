with country as (
    select {{ dbt_utils.star(ref('stg_nation')) }}
    from {{ ref('stg_nation') }}
)
, region as (
    select {{ dbt_utils.star(ref('stg_region')) }}
    from {{ ref('stg_region') }}
)
select 
    country_id,
    country_name,
    region_name
from country
left join region
on country.country_region_id = region.region_id 