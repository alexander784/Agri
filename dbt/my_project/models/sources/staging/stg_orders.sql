WITH source AS (
    SELECT * 
    FROM {{ source('dev', 'orders') }}
),

renamed AS (
    SELECT
        id AS order_record_id,
        order_id,
        customer_id,
        product_id,
        quantity,
        date::timestamp AS order_date,
        INITCAP(TRIM(city)) AS delivery_city
    FROM source
)

SELECT * FROM renamed
