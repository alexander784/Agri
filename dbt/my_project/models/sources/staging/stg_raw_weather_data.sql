WITH source AS (
    SELECT * 
    FROM {{ source('dev', 'raw_weather_data') }}
),

renamed AS (
    SELECT
        id AS weather_id,
        INITCAP(TRIM(city)) AS city,            
        ROUND(temperature::numeric, 2) AS temperature,    
        LOWER(TRIM(weather_descriptions)) AS weather_description,
        ROUND(wind_speed::NUMERIC, 2) AS wind_speed,
        time::timestamp AS recorded_at,
        inserted_at::timestamp AS inserted_at,
        utc_offset
    FROM source
)

SELECT * FROM renamed
