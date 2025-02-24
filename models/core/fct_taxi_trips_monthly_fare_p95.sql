{{ config(materialized='table') }}

WITH filtered_trips AS (
    SELECT 
        service_type, 
        fare_amount,
        pickup_year, 
        pickup_month
    FROM {{ ref('fact_trips') }}
    WHERE pickup_year = 2020
    and pickup_month = 4
    and (  fare_amount > 0   
         or trip_distance > 0
         or payment_type_description IN ('Cash', 'Credit Card')
)),

percentiles AS (
SELECT
    service_type,
    pickup_year,
    pickup_month,
    PERCENTILE_CONT(fare_amount, 0.90) OVER( partition by service_type, pickup_year, pickup_month)  AS p90,
    PERCENTILE_CONT(fare_amount, 0.95)  OVER( partition by service_type, pickup_year, pickup_month) AS p95,
    PERCENTILE_CONT(fare_amount, 0.97)  OVER( partition by service_type, pickup_year, pickup_month) AS p97 
FROM filtered_trips

)

SELECT * 
FROM percentiles
GROUP BY service_type, pickup_year, pickup_month,p90,p95,p97
ORDER BY service_type;
