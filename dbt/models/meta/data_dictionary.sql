-- Static data dictionary for the whole lakehouse. One row per (table, column)
-- with an English description. Hand-maintained: when a column is added/renamed in
-- a bronze / staging / silver / gold / ops table, add or edit
-- the matching row here. Materialized as an Iceberg table (lakehouse.meta) so it
-- is queryable from Spark Thrift / Superset:  SELECT * FROM lakehouse.meta.data_dictionary
--
-- table_name is fully qualified (catalog.namespace.table); staging models are
-- Spark views in the default catalog so they are shown as staging.<model>.

SELECT table_name, column_name, description
FROM VALUES
    -- ============================ bronze (pyspark streaming) ============================
    -- Every bronze table shares the same raw-payload schema: CDC metadata + dedup PK + raw JSON.
    ('lakehouse.bronze.orders',       'op',            'CDC operation for this event: c=create, u=update, r=snapshot read, d=delete.'),
    ('lakehouse.bronze.orders',       'lsn',           'Postgres WAL log sequence number. Sole ordering key for CDC dedup (never created_at).'),
    ('lakehouse.bronze.orders',       'ts_ms',         'Debezium source commit timestamp in epoch milliseconds. Tie-breaker after lsn.'),
    ('lakehouse.bronze.orders',       'order_id',      'Dedup partition key. COALESCE(after.order_id, before.order_id) so deletes keep the key.'),
    ('lakehouse.bronze.orders',       'raw_payload',   'Entire Debezium payload after-image (before-image on delete) stored as a raw JSON string.'),
    ('lakehouse.bronze.users',        'op',            'CDC operation for this event: c=create, u=update, r=snapshot read, d=delete.'),
    ('lakehouse.bronze.users',        'lsn',           'Postgres WAL log sequence number. Sole ordering key for CDC dedup.'),
    ('lakehouse.bronze.users',        'ts_ms',         'Debezium source commit timestamp in epoch milliseconds.'),
    ('lakehouse.bronze.users',        'user_id',       'Dedup partition key. COALESCE(after.user_id, before.user_id).'),
    ('lakehouse.bronze.users',        'raw_payload',   'Entire Debezium payload as a raw JSON string.'),
    ('lakehouse.bronze.products',     'op',            'CDC operation for this event: c=create, u=update, r=snapshot read, d=delete.'),
    ('lakehouse.bronze.products',     'lsn',           'Postgres WAL log sequence number. Sole ordering key for CDC dedup.'),
    ('lakehouse.bronze.products',     'ts_ms',         'Debezium source commit timestamp in epoch milliseconds.'),
    ('lakehouse.bronze.products',     'product_id',    'Dedup partition key. COALESCE(after.product_id, before.product_id).'),
    ('lakehouse.bronze.products',     'raw_payload',   'Entire Debezium payload as a raw JSON string.'),
    ('lakehouse.bronze.order_items',  'op',            'CDC operation for this event: c=create, u=update, r=snapshot read, d=delete.'),
    ('lakehouse.bronze.order_items',  'lsn',           'Postgres WAL log sequence number. Sole ordering key for CDC dedup.'),
    ('lakehouse.bronze.order_items',  'ts_ms',         'Debezium source commit timestamp in epoch milliseconds.'),
    ('lakehouse.bronze.order_items',  'order_item_id', 'Dedup partition key. COALESCE(after.order_item_id, before.order_item_id).'),
    ('lakehouse.bronze.order_items',  'raw_payload',   'Entire Debezium payload as a raw JSON string.'),

    -- ============================ staging (dbt views, default catalog) ============================
    -- Deduped, JSON-extracted latest-state views. Ephemeral: rebuilt every dbt run.
    ('staging.stg_orders',        'order_id',      'Order identifier. Latest deduped state per order (lsn DESC).'),
    ('staging.stg_orders',        'user_id',       'Customer who placed the order.'),
    ('staging.stg_orders',        'status',        'Order lifecycle status: CREATED, PAID or CANCELLED.'),
    ('staging.stg_orders',        'total_amount',  'Order total amount, DECIMAL(12,2), extracted from raw_payload.'),
    ('staging.stg_orders',        'created_at',    'Order creation timestamp (set once at INSERT; do not use it for CDC ordering).'),
    ('staging.stg_orders',        'is_deleted',    'True when the latest CDC event for this order was a delete (op=d). Soft delete.'),
    ('staging.stg_users',         'user_id',       'Customer identifier. Latest deduped state per user.'),
    ('staging.stg_users',         'full_name',     'Customer full name.'),
    ('staging.stg_users',         'city',          'Customer city.'),
    ('staging.stg_users',         'created_at',    'User creation timestamp.'),
    ('staging.stg_users',         'is_deleted',    'True when the latest CDC event for this user was a delete. Soft delete.'),
    ('staging.stg_products',      'product_id',    'Product identifier. Latest deduped state per product.'),
    ('staging.stg_products',      'name',          'Product name.'),
    ('staging.stg_products',      'category',      'Product category.'),
    ('staging.stg_products',      'price',         'Product unit price, DECIMAL(10,2).'),
    ('staging.stg_products',      'is_deleted',    'True when the latest CDC event for this product was a delete. Soft delete.'),
    ('staging.stg_order_items',   'order_item_id', 'Order line identifier. Latest deduped state per line.'),
    ('staging.stg_order_items',   'order_id',      'Parent order of this line.'),
    ('staging.stg_order_items',   'product_id',    'Product purchased on this line.'),
    ('staging.stg_order_items',   'quantity',      'Quantity ordered on this line.'),
    ('staging.stg_order_items',   'unit_price',    'Unit price captured on this line, DECIMAL(10,2).'),
    ('staging.stg_order_items',   'is_deleted',    'True when the latest CDC event for this line was a delete. Soft delete.'),

    -- ============================ silver (lakehouse.silver, dbt tables) ============================
    ('lakehouse.silver.core_orders',      'order_id',     'Order identifier. Unique, not null.'),
    ('lakehouse.silver.core_orders',      'user_id',      'Customer who placed the order.'),
    ('lakehouse.silver.core_orders',      'full_name',    'Customer full name, joined from stg_users.'),
    ('lakehouse.silver.core_orders',      'city',         'Customer city, joined from stg_users.'),
    ('lakehouse.silver.core_orders',      'status',       'Order lifecycle status: CREATED, PAID or CANCELLED.'),
    ('lakehouse.silver.core_orders',      'total_amount', 'Order total amount.'),
    ('lakehouse.silver.core_orders',      'created_at',   'Order creation timestamp.'),
    ('lakehouse.silver.core_orders',      'is_deleted',   'True when the latest event was a delete (soft delete). Excluded from marts.'),
    ('lakehouse.silver.core_orders',      'paid_amount',  'total_amount when status=PAID, else 0. Revenue basis for the marts.'),
    ('lakehouse.silver.core_orders',      'is_cancelled', '1 when status=CANCELLED, else 0.'),
    ('lakehouse.silver.core_order_items', 'order_item_id','Order line identifier. Unique, not null.'),
    ('lakehouse.silver.core_order_items', 'order_id',     'Parent order of this line.'),
    ('lakehouse.silver.core_order_items', 'product_id',   'Product purchased on this line.'),
    ('lakehouse.silver.core_order_items', 'product_name', 'Product name, joined from stg_products.'),
    ('lakehouse.silver.core_order_items', 'category',     'Product category, joined from stg_products.'),
    ('lakehouse.silver.core_order_items', 'quantity',     'Quantity ordered on this line.'),
    ('lakehouse.silver.core_order_items', 'unit_price',   'Unit price captured on this line.'),
    ('lakehouse.silver.core_order_items', 'line_total',   'quantity * unit_price. Line revenue.'),
    ('lakehouse.silver.core_order_items', 'is_deleted',   'True when the latest event was a delete (soft delete). Excluded from marts.'),

    -- ============================ gold (lakehouse.gold, dbt marts) ============================
    ('lakehouse.gold.mart_daily_revenue',     'order_date',       'Calendar day (from created_at) of the aggregated orders.'),
    ('lakehouse.gold.mart_daily_revenue',     'total_orders',     'Count of non-deleted orders on that day.'),
    ('lakehouse.gold.mart_daily_revenue',     'total_revenue',    'Sum of paid_amount (PAID orders only) on that day.'),
    ('lakehouse.gold.mart_daily_revenue',     'cancelled_orders', 'Count of cancelled orders on that day.'),
    ('lakehouse.gold.mart_daily_revenue',     'unique_customers', 'Distinct customers who ordered on that day.'),
    ('lakehouse.gold.mart_sales_by_category', 'category',         'Product category.'),
    ('lakehouse.gold.mart_sales_by_category', 'total_orders',     'Distinct PAID, non-deleted orders containing this category.'),
    ('lakehouse.gold.mart_sales_by_category', 'total_quantity',   'Total quantity sold in this category.'),
    ('lakehouse.gold.mart_sales_by_category', 'total_revenue',    'Total line revenue (sum of line_total) for this category.'),

    -- ============================ ops (lakehouse.ops, stock-monitor consumer) ============================
    ('lakehouse.ops.stock_alerts',       'product_id',          'Product that dropped to or below the low-stock threshold.'),
    ('lakehouse.ops.stock_alerts',       'stock_qty',           'Remaining stock quantity at the time of the alert.'),
    ('lakehouse.ops.stock_alerts',       'threshold',           'Low-stock threshold that triggered the alert.'),
    ('lakehouse.ops.stock_alerts',       'alert_time',          'Timestamp the alert was raised (current_timestamp at write).'),

    -- ============================ meta (this table) ============================
    ('lakehouse.meta.data_dictionary',   'table_name',          'Fully qualified table name (staging models shown as staging.<model>).'),
    ('lakehouse.meta.data_dictionary',   'column_name',         'Column name within the table.'),
    ('lakehouse.meta.data_dictionary',   'description',         'English description of the column.')
AS dict(table_name, column_name, description)
