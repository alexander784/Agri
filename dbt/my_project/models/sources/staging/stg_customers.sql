{{ config(materialized='view') }}

SELECT
    id,
    firstname,
    lastname,
    email,
    phone,
    birthday,
    gender,
    address,
    zip,
    city
FROM {{ source('dev', 'customers') }}
WHERE email IS NOT NULL 