{{ config(materialized='view') }}

SELECT
    id,
    title,
    price,
    description,
    category
FROM {{ source('dev', 'products') }}
WHERE title IS NOT NULL AND price > 0