with base as (
    select
        date_trunc(registration_date, month) as month,
        ev_category
    from {{ ref('int_registrations_enriched') }}
)

select
    month,
    count(*) as total_registrations,
    count_if(ev_category = 'BEV') as bev_count,
    count_if(ev_category = 'PHEV') as phev_count,
    count_if(ev_category = 'HEV') as hev_count,
    count_if(ev_category = 'ICE') as ice_count,
    count_if(ev_category = 'BEV') / count(*) as bev_share
from base
group by month
order by month
