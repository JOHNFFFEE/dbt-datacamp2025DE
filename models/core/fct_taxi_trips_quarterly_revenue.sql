{{ config(
    materialized='table'
) }}

WITH quarterly_revenue AS (
    SELECT
        pickup_year_quarter, 
        pickup_year,
        pickup_quarter,
        service_type,  -- Green Taxi / Yellow Taxi
        SUM(total_amount) AS quarter_total_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY 1, 2, 3, 4
),

yoy_growth AS (
    SELECT
        q1.service_type,
        q1.pickup_year_quarter,
        q1.pickup_year,
        q1.pickup_quarter,        
        q1.quarter_total_revenue,
        q2.quarter_total_revenue AS prev_year_revenue,
        -- Compute Year-over-Year Growth
        ROUND(
            (q1.quarter_total_revenue - q2.quarter_total_revenue) / NULLIF(q2.quarter_total_revenue, 0) * 100, 2
        ) AS yoy_growth_percentage
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.service_type = q2.service_type
        AND q1.pickup_year = q2.pickup_year + 1  -- Match with previous year's quarter
        AND q1.pickup_quarter = q2.pickup_quarter
  --  where q1.pickup_year in (2019,2020)
)

SELECT * FROM yoy_growth ;

