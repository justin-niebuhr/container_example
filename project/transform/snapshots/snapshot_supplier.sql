
{% snapshot snapshot_supplier %}

{{
    config(
        target_schema='snapshots',
        unique_key='supplier',
        strategy='timestamp',
        updated_at='created_at'
    )
}}
        SELECT  DISTINCT     
            supplier,
            created_at
        FROM {{ source('example_source', 'warehouse_and_retail_sales') }}

{% endsnapshot %}