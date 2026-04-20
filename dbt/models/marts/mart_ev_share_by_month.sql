with base as (
    select
        registration_date,
        ev_category,
        registrations
    from {{ ref('int_registrations_enriched') }}
)

select
    date_trunc(registration_date, month) as month,

    sum(registrations) as total_registrations,

    sum(case when ev_category = 'BEV' then registrations else 0 end) as bev_count,
    sum(case when ev_category = 'PHEV' then registrations else 0 end) as phev_count,
    sum(case when ev_category = 'HEV' then registrations else 0 end) as hev_count,
    sum(case when ev_category = 'ICE' then registrations else 0 end) as ice_count,

    -- BEV share only
    sum(case when ev_category = 'BEV' then registrations else 0 end)
        / sum(registrations) as bev_share,

    -- EV share (BEV + PHEV + HEV)
    (sum(case when ev_category in ('BEV','PHEV','HEV') then registrations else 0 end))
        / sum(registrations) as ev_share

from base
group by month
order by month
