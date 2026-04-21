with base as (
    select *
    from {{ ref('stg_registrations') }}
),

-- Step A: normalize make names
make_map as (
    select
        make_raw,
        make_standardized
    from {{ ref('lk_make_map') }}
),

-- Step B: normalize model names
model_map as (
    select
        make_standardized,
        model_raw,
        model_standardized
    from {{ ref('lk_model_map') }}
)

select
    b.*,

    -- Clean make (fallback to raw)
    coalesce(mm.make_standardized, b.make) as make_clean,

    -- Clean model (fallback to raw)
    coalesce(md.model_standardized, b.model_raw) as model_clean

from base b

-- Join on make_raw to fix make names
left join make_map mm
    on upper(trim(b.make)) = upper(trim(mm.make_raw))

-- Join on model_raw to fix model names
left join model_map md
    on upper(trim(b.model_raw)) = upper(trim(md.model_raw))
