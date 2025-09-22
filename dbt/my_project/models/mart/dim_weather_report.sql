WITH base AS (
    SELECT *
    FROM {{ ref('stg_weather_data') }}
),

daily_summary AS (
    SELECT
        city,
        DATE(inserted_at) AS date,

        ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
        ROUND(MAX(temperature)::numeric, 2) AS max_temperature,
        ROUND(MIN(temperature)::numeric, 2) AS min_temperature,

        ROUND(AVG(wind_speed)::numeric, 2) AS avg_wind_speed,
        COUNT(*) AS total_readings,

        MODE() WITHIN GROUP (ORDER BY weather_descriptions) AS most_common_condition
    FROM base
    GROUP BY city, DATE(inserted_at)
),

latest_timestamp AS (
    SELECT 
        city,
        MAX(inserted_at) AS last_inserted_at
    FROM base
    GROUP BY city
)

SELECT 
    ds.city,
    ds.date,
    ds.avg_temperature,
    ds.max_temperature,
    ds.min_temperature,
    ds.avg_wind_speed,
    ds.total_readings,
    ds.most_common_condition,
    lt.last_inserted_at
FROM daily_summary ds
LEFT JOIN latest_timestamp lt 
    ON ds.city = lt.city
