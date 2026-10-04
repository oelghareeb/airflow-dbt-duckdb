WITH source AS (
    SELECT * FROM read_csv_auto('/workspaces/airflow-dbt-duckdb/data/raw_orders.csv')
)
SELECT
    CAST(order_id AS INT) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(amount AS DECIMAL(10,2)) AS amount,
    UPPER(status) AS status
FROM source
WHERE status != 'CANCELLED'