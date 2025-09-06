WITH source AS (
    SELECT * 
    FROM {{ source('dev', 'customers') }}
),

renamed AS (
    SELECT
        id AS customer_id,
        LOWER(TRIM(firstname)) AS first_name,
        LOWER(TRIM(lastname)) AS last_name,
        LOWER(TRIM(email)) AS email,
        phone,
        birthday::date AS birth_date,
        INITCAP(TRIM(gender)) AS gender,
        TRIM(address) AS street_address,
        zip AS postal_code,
        INITCAP(TRIM(city)) AS city
    FROM source
)

SELECT * FROM renamed
