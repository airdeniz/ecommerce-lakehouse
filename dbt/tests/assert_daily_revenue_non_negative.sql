-- Revenue must never be negative on any day.
-- If any row is returned, the test FAILS.
SELECT
    order_date,
    total_revenue
FROM {{ ref('mart_daily_revenue') }}
WHERE total_revenue < 0
