{{
    config(
        materialized='view'
    )
}}

with fhv2019 as 
(
  select *,
  from {{ source('staging','FHV2019') }}
  where dispatching_base_num is not null
)

SELECT *
FROM fhv2019