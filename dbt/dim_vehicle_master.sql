{{ config(
    materialized = "table",
    schema = "vic_vehicle_analytics"
) }}

WITH stg AS (
    SELECT
        make_raw,
        model_raw,
        registration_year,
        registration_month,
        fuel_type,
        body_type,
        vehicle_class,
        registrations
    FROM {{ source('vic', 'stg_monthly_vehicle_registration') }}
),

decoded AS (
    SELECT
        make_raw,
        model_raw,
        make_clean,
        model_clean,
        vehicle_type
    FROM {{ source('vic', 'dim_vehicle_decoded') }}
),

ev AS (
    SELECT
        make_clean,
        model_clean,
        ev_category
    FROM {{ source('vic', 'dim_vehicle_ev') }}
)

SELECT
    -- Cleaned attributes
    d.make_clean,
    d.model_clean,
    d.vehicle_type,
    COALESCE(e.ev_category, 'ICE') AS ev_category,

    -- Registration facts (VIC only)
    s.registration_year,
    s.registration_month,
    s.registrations,

    -- Additional metadata
    s.fuel_type,
    s.body_type,
    s.vehicle_class,

    -- Business key
    CONCAT(d.make_clean, '_', d.model_clean) AS vehicle_key

FROM decoded d
LEFT JOIN stg s
    ON d.make_raw = s.make_raw
   AND d.model_raw = s.model_raw
LEFT JOIN ev e
    ON d.make_clean = e.make_clean
   AND d.model_clean = e.model_clean;
