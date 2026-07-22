WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

final AS (
    SELECT
        o.order_id,
        o.user_id,
        u.full_name,
        u.city,
        o.status,
        o.total_amount,
        o.created_at,
        o.is_deleted,
        -- Revenue is recognised for any paid, not-yet-reversed state (PAID and
        -- the post-payment fulfilment states), not just PAID. See the
        -- revenue_statuses() macro for the full rationale.
        CASE
            WHEN o.status IN {{ revenue_statuses() }} THEN o.total_amount
            ELSE 0
        END AS paid_amount,
        CASE
            WHEN o.status = 'CANCELLED' THEN 1
            ELSE 0
        END AS is_cancelled,
        CASE
            WHEN o.status = 'RETURNED' THEN 1
            ELSE 0
        END AS is_returned,
        CASE
            WHEN o.status = 'REFUNDED' THEN 1
            ELSE 0
        END AS is_refunded
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    -- We keep every lifecycle status (CREATED, PAID, PREPARING, SHIPPED,
    -- DELIVERED, CANCELLED, RETURNED, REFUNDED): each is a valid state and can
    -- be analysed (unpaid-cart, fulfilment funnel, return rate).
    -- Deleted records stay flagged with is_deleted=true (soft delete).
)

SELECT * FROM final