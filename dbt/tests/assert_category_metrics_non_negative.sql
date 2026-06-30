-- Category-level metrics cannot be negative.
-- If any row is returned, the test FAILS.
SELECT
    category,
    total_quantity,
    total_revenue
FROM {{ ref('mart_sales_by_category') }}
WHERE total_quantity < 0
   OR total_revenue < 0
