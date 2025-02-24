{{
    config(
        materialized='table'
    )
}}

with last_fhv as (
    select *
    from {{ ref('stg_fhv') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select *
from last_fhv
inner join dim_zones as pickup_zone
on last_fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on last_fhv.dropoff_locationid = dropoff_zone.locationid