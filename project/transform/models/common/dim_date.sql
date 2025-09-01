{{ config(materialized="table", unique_key="date_day") }}
with
    date_spine as (

        {{
            dbt_utils.date_spine(
                start_date="to_date('01/01/2017', 'mm/dd/yyyy')",
                datepart="day",
                end_date="current_date + INTERVAL '100 year'",
            )
        }}

    ),

    calculated as (

        select
            date_day,
            date_day as date_actual,
            date_part('month', date_day) as month_actual,
            date_part('year', date_day) as year_actual,
            date_part('year', date_day + INTERVAL '1 year' ) year_ago_1,
            date_part('month', date_day + INTERVAL '1 month' ) month_ago_1
        from date_spine



    ),

    current_date_information as (

        select
            true as is_current_year,
            true as is_current_month
        from calculated
        where current_date = date_actual

    ),
    final as (

        select
            calculated.date_day,
            calculated.date_actual,
            calculated.year_actual,
            calculated.month_actual,
            calculated.year_ago_1,
            calculated.month_ago_1,
            current_date_information.is_current_year,
            current_date_information.is_current_month
        from calculated
        cross join current_date_information

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
