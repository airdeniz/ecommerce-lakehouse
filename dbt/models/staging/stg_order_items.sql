WITH source AS (
    SELECT
        op,
        order_item_id,
        order_id,
        product_id,
        quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price
    FROM lakehouse.bronze.order_items
    WHERE op IN ('c', 'u', 'r')
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY order_item_id
        ) AS rn
    FROM source
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price
FROM deduped
WHERE rn = 1