with base as (
    select *
    from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`stg_registrations`
),

map as (
    select
        make_raw,
        make_standardized,
        model_raw,
        model_standardized
    from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`lk_model_map_clean`
)

select
    b.*,
    coalesce(m.make_standardized, b.make) as make_clean,
    coalesce(m.model_standardized, b.model_raw) as model_clean
from base b
left join map m
    on upper(trim(b.make)) = upper(trim(m.make_raw))
   and upper(trim(b.model_raw)) = upper(trim(m.model_raw))