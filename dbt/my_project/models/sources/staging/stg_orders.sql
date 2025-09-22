{{ config(materialized='view') }}

SELECT
    id,
    order_id,
    customer_id,
    product_id,
    quantity,
    date,
    city
FROM {{ source('dev', 'orders') }}
WHERE quantity > 0