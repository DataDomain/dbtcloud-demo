use database bnt_development
;
use role pc_dbt_role
;
use warehouse pc_dbt_wh
;

-- dates testing
select to_timestamp('31/12/2050', 'dd/mm/yyyy'), convert_timezone('Pacific/Auckland', current_timestamp())
    , convert_timezone('Pacific/Auckland', timestampadd( second, -1, current_timestamp()))
;
-- dim_customer
select count(*) from dbt_default.dim_customer
; --15K

select * from dbt_default.dim_customer limit 100
;

select count(*) from dbt_default.dim_customer_ct
; --15K

select customer_key, customer_id, customer_address, customer_phone,
dw_process_dt, dw_current_flag, dw_valid_from, dw_valid_to
from dbt_default.dim_customer_ct 
where 1 = 1 
--and DW_Current_flag = 'N'
and customer_id = 1
limit 100
;

-- test change tracking
update dbt_default.stg_customer
set customer_address = '46 Union Road, Howick'
where customer_id = 1
;

--reprocess the dim_customer_ct
