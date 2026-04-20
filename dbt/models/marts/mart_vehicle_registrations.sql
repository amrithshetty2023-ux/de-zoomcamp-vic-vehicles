select
    registration_id,
    registration_date,
    make,
    model_standardized,
    ev_category,
    body_type,
    state,
    year
from {{ ref('int_registrations_enriched') }}
