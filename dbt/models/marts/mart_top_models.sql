select
    make,
    model_standardized,
    ev_category,
    count(*) as registrations
from {{ ref('int_registrations_enriched') }}
group by make, model_standardized, ev_category
order by registrations desc
