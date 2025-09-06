WITH source AS (
    SELECT * 
    FROM {{ source('dev', 'products') }}
),

renamed AS (
    SELECT
        id AS product_id,
        INITCAP(TRIM(title)) AS product_name,
        ROUND(price::numeric, 2) AS price,
        TRIM(description) AS description,
        LOWER(TRIM(category)) AS category
    FROM source
)

SELECT * FROM renamed
