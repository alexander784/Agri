{{ config(materialized='view') }}

SELECT
    o.id,
    o.order_id,
    o.customer_id,
    c.full_name AS customer_name,
    o.product_id,
    p.title AS product_title,
    o.quantity,
    o.date,
    o.city
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.id
JOIN {{ ref('dim_products') }} p ON o.product_id = p.id