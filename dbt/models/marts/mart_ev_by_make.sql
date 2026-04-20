select
    make,
    count_if(ev_category = 'BEV') as bev_count,
    count_if(ev_category = 'PHEV') as phev_count,
    count_if(ev_category = 'HEV') as hev_count,
    count_if(ev_category = 'ICE') as ice_count,
    count_if(ev_category = 'BEV') / count(*) as bev_share
from {{ ref('int_registrations_enriched') }}
group by make
order by bev_count desc
