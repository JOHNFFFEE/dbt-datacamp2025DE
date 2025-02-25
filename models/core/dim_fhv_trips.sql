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


select distinct last_fhv.dispatching_base_num , last_fhv.pickup_datetime , last_fhv.dropOff_datetime , last_fhv.PUlocationID, last_fhv.DOlocationID, last_fhv.Affiliated_base_number,
 pickup.zone as pickupZone, dropoff_zone.zone as dropoffZone,
EXTRACT ( month from last_fhv.pickup_datetime) as pickup_month,    
EXTRACT (year from last_fhv.pickup_datetime) as pickup_year,
from last_fhv
inner join dim_zones as pickup
on last_fhv.PUlocationID = pickup.locationid
inner join dim_zones as dropoff_zone
on last_fhv.DOlocationID = dropoff_zone.locationid
