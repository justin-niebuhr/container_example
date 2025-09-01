{{ config(materialized="table", unique_key="supplier_id") }}
/*
Logic not ideal as just picking a recent value"
*/
with
    final as (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['supplier', 'dbt_valid_from']) }} as SUPPLIER_SCD2_ID,
            {{ dbt_utils.generate_surrogate_key(["supplier"]) }} as supplier_id,
            supplier as supplier_name,
            row_number() over (
                partition by supplier order by dbt_valid_from
            ) as version,
            case when dbt_valid_to is null then true else false end as is_current,
            dbt_valid_from,
            case
                when dbt_valid_to is null then to_date('9999-12-31','YYYY-MM-DD')
            end as dbt_valid_to
        FROM {{ ref("snapshot_supplier") }}
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