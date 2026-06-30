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
        CASE
            WHEN o.status = 'PAID' THEN o.total_amount
            ELSE 0
        END AS paid_amount,
        CASE
            WHEN o.status = 'CANCELLED' THEN 1
            ELSE 0
        END AS is_cancelled
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    -- We keep all statuses including CREATED: CREATED is a valid lifecycle
    -- state and can be analysed (e.g. unpaid-cart analysis).
    -- Deleted records stay flagged with is_deleted=true (soft delete).
)

SELECT * FROM final