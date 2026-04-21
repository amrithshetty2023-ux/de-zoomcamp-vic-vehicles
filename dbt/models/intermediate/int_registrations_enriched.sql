with base as (
    select *
    from {{ ref('int_normalized_make_model') }}
),

ev as (
    select
        make_standardized,
        model_standardized,
        ev_category
    from {{ ref('lk_ev_model_map') }}
)

select
    b.registration_id,
    b.registration_date,
    b.make_clean as make,
    b.model_clean as model_standardized,
    b.body_type,
    b.state,
    b.year,
    b.registrations,
    ev.ev_category
from base b
left join ev
    on b.make_clean = ev.make_standardized
   and b.model_clean = ev.model_standardized
