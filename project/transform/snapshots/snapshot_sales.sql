
{% snapshot snapshot_sales %}

{{
    config(
        target_schema='snapshots',
        unique_key='composit_key',
        strategy='timestamp',
        updated_at='created_at'
    )
}}

select       
    CONCAT(calendar_year, calendar_month, supplier, item_code) as composit_key,
    *
  from {{ source('example_source', 'warehouse_and_retail_sales') }}

{% endsnapshot %}