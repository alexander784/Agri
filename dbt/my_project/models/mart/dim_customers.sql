{{ config(materialized='view') }}

SELECT
    id,
    CONCAT(firstname, ' ', lastname) AS full_name,
    email,
    city
FROM {{ ref('stg_customers') }}