{% snapshot customer_snapshot %}

{{
    config(
      target_schema= 'dbt_default',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='DW_PROCESS_DT',
    )
}}

select * from {{ ref("dim_customer") }}

{% endsnapshot %}