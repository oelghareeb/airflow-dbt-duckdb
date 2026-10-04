WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(amount) AS total_revenue
FROM stg_orders
WHERE status = 'COMPLETED'
GROUP BY order_date