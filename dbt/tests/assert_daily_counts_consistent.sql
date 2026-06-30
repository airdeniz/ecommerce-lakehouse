-- Logical consistency:
--   cancelled count <= total orders
--   unique customers <= total orders
-- If a day violating these is returned, the test FAILS.
SELECT
    order_date,
    total_orders,
    cancelled_orders,
    unique_customers
FROM {{ ref('mart_daily_revenue') }}
WHERE cancelled_orders > total_orders
   OR unique_customers > total_orders
