with monthly as (
    select
        date_trunc(registration_date, month) as month,
        countif(ev_category = 'BEV') as bev_count
    from {{ ref('int_registrations_enriched') }}
    group by month
)

select
    month,
    bev_count,
    bev_count - lag(bev_count) over (order by month) as bev_growth_mom,
    bev_count - lag(bev_count, 12) over (order by month) as bev_growth_yoy
from monthly
order by month
