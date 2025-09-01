
{% snapshot snapshot_item %}

{{
    config(
        target_schema='snapshots',
        unique_key='item_code',
        strategy='timestamp',
        updated_at='created_at'
    )
}}

    SELECT 
        item_code,
        item_description,
        item_type,
        created_at
    FROM(
        SELECT       
            item_code,
            item_description,
            item_type,
            created_at,
            ROW_NUMBER() OVER (PARTITION BY item_code ORDER BY calendar_year,calendar_month,created_at DESC) as rn
        FROM {{ source('example_source', 'warehouse_and_retail_sales') }}
    )
    where rn =1

{% endsnapshot %}