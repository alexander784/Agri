{{ config(materialized='view') }}

SELECT
    id,
    city,
    temperature,
    weather_descriptions,
    wind_speed,
    time,
    inserted_at,
    utc_offset
FROM {{ source('dev', 'raw_weather_data') }}
WHERE city IS NOT NULL AND time IS NOT NULL