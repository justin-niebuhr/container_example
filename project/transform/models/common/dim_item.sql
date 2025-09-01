{{ config(materialized="table", unique_key="item_id") }}
/*
Logic not ideal as just picking a recent value"
*/
with
    time_frame as (
        SELECT
            item_code,
            min(calendar_year || LPAD(calendar_month::TEXT,2,'00')) as first_occurrence,
            max(calendar_year || LPAD(calendar_month::TEXT,2,'00')) as last_occurrence
        FROM {{ ref("snapshot_sales") }}
        GROUP BY item_code
    ),

    calculate as (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['item_code', 'dbt_valid_from']) }} as ITEM_SCD2_ID,
            {{ dbt_utils.generate_surrogate_key(["item_code"]) }} as item_id,
            item_code,
            item_description,
            item_type,
             row_number() over (
                partition by item_code order by dbt_valid_from
            ) as version,
            case when dbt_valid_to is null then true else false end as is_current,
            dbt_valid_from,
            case
                when dbt_valid_to is null then to_date('9999-12-31','YYYY-MM-DD')
            end as dbt_valid_to
        FROM {{ ref("snapshot_item") }}
    ),

    final as (
        SELECT
            c.*,
            tf.first_occurrence,
            tf.last_occurrence
        FROM calculate c
        LEFT OUTER JOIN time_frame tf on tf.item_code =  c.item_code
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