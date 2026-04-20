select
    -- Create a surrogate key because raw data has no unique ID
    concat(
        CD_MAKE_VEH, ':',
        CD_MODEL_VEH, ':',
        cast(registration_month as string), ':',
        CD_CLR_BDY_VEH_P
    ) as registration_id,

    -- Convert registration_month (DATE) into a proper date field
    registration_month as registration_date,

    -- Standardize naming
    upper(trim(CD_MAKE_VEH)) as make,
    upper(trim(CD_MODEL_VEH)) as model_raw,
    upper(trim(CD_CLR_BDY_VEH_P)) as body_type,

    -- VIC dataset always has VIC, but keep it for consistency
    'VIC' as state,

    NB_YEAR_MFC_VEH as year,

    -- No fuel_type in raw data → leave null for now
    null as fuel_type,

    -- Keep TOTAL (number of registrations for that model/month)
    TOTAL as registrations
from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`ext_monthly_vehicle_registration_raw`