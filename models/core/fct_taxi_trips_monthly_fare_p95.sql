{{ config(materialized="table") }}

with
    filtered_trips as (
        select
            service_type,
            fare_amount,
            trip_distance,  -- Make sure to include trip_distance to check the filtering condition
            pickup_year,
            pickup_month,
            payment_type_description
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0  -- Include records where trip_distance is missing
            and payment_type_description in ('Cash', 'Credit Card')
    ),

    percentiles as (
        select
            service_type,
            percentile_cont(fare_amount, 0.97) over (
                partition by service_type, pickup_year, pickup_month
            ) as p97,
            percentile_cont(fare_amount, 0.95) over (
                partition by service_type, pickup_year, pickup_month
            ) as p95,
            percentile_cont(fare_amount, 0.90) over (
                partition by service_type, pickup_year, pickup_month
            ) as p90
        from filtered_trips
        where pickup_year = 2020 and pickup_month = 4
    )

select distinct service_type, p97, p95, p90
from percentiles
order by service_type
