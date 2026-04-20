with base as (
    select *
    from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`stg_registrations`
),

lk as (
    select
        make_standardized,
        model_standardized,
        ev_category
    from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`lk_ev_classification_clean`
)

select
    b.registration_id,
    b.registration_date,
    b.make,
    b.model_raw,
    l.model_standardized,
    l.ev_category,
    b.body_type,
    b.state,
    b.year
from base b
left join lk l
    on b.make = l.make_standardized
   and upper(trim(b.model_raw)) = l.model_standardized