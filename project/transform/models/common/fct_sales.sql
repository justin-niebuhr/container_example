{{ config(materialized="incremental", unique_key="sales_id") }}

with
    snapshot_sales as (
        select
            
            calendar_year,
            calendar_month,
            supplier as supplier_name,
            item_code,
            item_description,
            item_type,
            retail_sales,
            retail_transfers, 
            warehouse_sales,
            created_at,
            dbt_valid_from
        from {{ ref("snapshot_sales") }}
        where dbt_valid_to is null  -- get latest version of each row
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['calendar_year','calendar_month','supplier_name','item_code', 'dbt_valid_from']) }} as sales_id,
            calendar_year,
            calendar_month,
            supplier_name,
            {{ dbt_utils.generate_surrogate_key(["supplier_name"]) }} as supplier_id,
            {{ dbt_utils.generate_surrogate_key(["item_code"]) }} as item_id,
            item_code,
            item_description,
            item_type,
            retail_sales,
            retail_transfers, 
            warehouse_sales,
            created_at
        from snapshot_sales
        {% if is_incremental() %}

            where calendar_year || LPAD(calendar_month::TEXT,2,'00') >= (select max(calendar_year || LPAD(calendar_month::TEXT,2,'00')) from {{ this }})

        {% endif %}
    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@gitlabs",
            updated_by="@jniebuhr",
            created_date="2025-08-21",
            updated_date="2025-08-21",
        )
    }}

