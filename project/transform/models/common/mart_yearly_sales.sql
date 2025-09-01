{{ config(materialized="incremental", unique_key="metric_id") }}

with
    min_yearmonth as (
        SELECT
            MIN(first_occurrence) as yearmonth
        FROM {{ ref("dim_item") }}
    ),
    dim_date as (
        SELECT DISTINCT
            year_actual, 
            year_ago_1
        FROM {{ ref("dim_date") }}
        WHERE date_day <= current_date
        AND date_day >= ( 
            SELECT 
                MIN(date_day) as fist_day_date
            FROM {{ ref("dim_date") }}
            WHERE year_actual = (SELECT left(yearmonth,4)::INT FROM min_yearmonth) 
            AND month_actual = (SELECT right(yearmonth,2)::INT FROM min_yearmonth)
        )
    ),
    dim_item as (
        SELECT
            item_id,
            item_type
        FROM {{ ref("dim_item") }}
        WHERE is_current
    ),
    item_type as (
        SELECT DISTINCT
            item_type
        FROM dim_item

    ),
    dim_supplier as (
        SELECT
            supplier_id,
            supplier_name
        FROM {{ ref("dim_supplier") }}
        WHERE is_current
    ),
    fct_sales as (
        SELECT
            calendar_year,
            supplier_id,
            i.item_type,
            count(*) as incident_count,
            sum(retail_sales) as total_retail_sales,
            sum(retail_transfers) as total_retail_transfers,
            sum(warehouse_sales) as total_warehouse_sales
        FROM {{ ref("fct_sales") }} s
        Left OUTER JOIN dim_item i on i.item_id = s.item_id
        GROUP BY 
            calendar_year,
            supplier_id,
            i.item_type
    ),
    dim_matrix as (
        SELECT
            *
        FROM dim_date d
        INNER JOIN item_type i ON 1=1
        INNER JOIN dim_supplier s ON 1=1
    ),

    calculate as (
        SELECT 
            {{ dbt_utils.generate_surrogate_key(["m.year_actual","m.supplier_id","m.item_type"]) }} as metric_id,
            m.year_actual,
            m.supplier_name,
            m.item_type,
            COALESCE(s_current.incident_count,0) AS incident_count,
            COALESCE(s_current.total_retail_sales,0) AS total_retail_sales,
            COALESCE(s_current.total_retail_transfers,0) AS total_retail_transfers,
            COALESCE(s_current.total_warehouse_sales,0) AS total_warehouse_sales,
            COALESCE(s_prior.incident_count,0) as prior_year_incident_count,
            COALESCE(s_prior.total_retail_sales,0) as prior_year_total_retail_sales,
            COALESCE(s_prior.total_retail_transfers,0) as prior_year_total_retail_transfers,
            COALESCE(s_prior.total_warehouse_sales,0) as prior_year_total_warehouse_sales
        FROM dim_matrix m
        LEFT OUTER JOIN fct_sales s_current on s_current.calendar_year = m.year_actual and s_current.supplier_id = m.supplier_id and s_current.item_type = m.item_type
        LEFT OUTER JOIN fct_sales s_prior on s_prior.calendar_year = m.year_ago_1 and s_prior.supplier_id = m.supplier_id and s_prior.item_type = m.item_type

    ),

    final as (
        SELECT
             metric_id,
            year_actual,
            supplier_name,
            item_type,
            incident_count,
            total_retail_sales,
            total_retail_transfers,
            total_warehouse_sales
            prior_year_incident_count,
            prior_year_total_retail_sales,
            prior_year_total_retail_transfers,
            prior_year_total_warehouse_sales
        FROM calculate
        {% if is_incremental() %}

            WHERE calendar_year  >= (select max(calendar_year) from {{ this }})

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

