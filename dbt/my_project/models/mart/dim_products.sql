{{ config(materialized='view') }}

SELECT
    id,
    title,
    price,
    category
FROM {{ ref('stg_products') }}