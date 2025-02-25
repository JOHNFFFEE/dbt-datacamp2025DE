{{
    config(
        materialized='table'
    )
}}


WITH dim_table AS (
select *,
TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, SECOND) as trip_duration
    FROM {{ ref('dim_fhv_trips') }}
),

percentiles as (
select 
--Affiliated_base_number,
dispatching_base_num,
pickupZone, dropoffZone,
pickup_year, 
pickup_month, 
CAST(PUlocationID AS INT64) AS PUlocationIDs, 
CAST(DOlocationID AS INT64) AS DOlocationIDs,
trip_duration
from dim_table
),

filtered_trips as (
select  distinct  pickupZone, dropoffZone,
PERCENTILE_CONT(trip_duration, 0.90) OVER( partition by pickup_year, pickup_month, PUlocationIDs, DOlocationIDs )  AS p90,
from percentiles as p
where pickup_month = 11 
and pickupZone IN ("Newark Airport", "SoHo", "Yorkville East")
),

pp as (
select  *,
   ROW_NUMBER() OVER (PARTITION BY pickupZone ORDER BY p90 DESC) AS row_num 
from filtered_trips
)

select *
from pp
where row_num <=2
order by pickupZone, row_num asc  