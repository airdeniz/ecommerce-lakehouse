-- Reconciliation:
-- mart_daily_revenue.total_revenue must equal the sum of paid_amount in
-- core_orders per day. Rounding tolerance is 0.01.
-- If a difference is returned, the test FAILS -> gold and silver disagree.
WITH mart AS (
    SELECT
        order_date,
        total_revenue
    FROM {{ ref('mart_daily_revenue') }}
),

core AS (
    SELECT
        DATE(created_at) AS order_date,
        SUM(paid_amount) AS revenue
    FROM {{ ref('core_orders') }}
    GROUP BY DATE(created_at)
)

SELECT
    m.order_date,
    m.total_revenue,
    c.revenue,
    ABS(m.total_revenue - c.revenue) AS diff
FROM mart m
FULL OUTER JOIN core c ON m.order_date = c.order_date
WHERE ABS(COALESCE(m.total_revenue, 0) - COALESCE(c.revenue, 0)) > 0.01
