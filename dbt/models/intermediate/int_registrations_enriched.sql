-- ============================================================
-- int_registrations_enriched
-- Enrich normalized registrations with EV category
-- Filters to passenger vehicles only
-- ============================================================

with base as (
    select *
    from {{ ref('int_normalized_make_model') }}
),

-- Filter to passenger makes only (using lookup table)
passenger as (
    select b.*
    from base b
    inner join {{ ref('lk_make_map') }} mm
        on b.make_clean = mm.make_standardized
),

ev as (
    select
        make_standardized,
        model_standardized,
        ev_category
    from {{ ref('lk_ev_model_map') }}
)

select
    p.registration_id,
    p.registration_date,
    p.make_clean as make,
    p.model_clean as model_standardized,
    p.body_type,
    p.state,
    p.year,
    p.registrations,
    coalesce(ev.ev_category, 'ICE') as ev_category
from passenger p
left join ev
    on p.make_clean = ev.make_standardized
   and p.model_clean = ev.model_standardized
