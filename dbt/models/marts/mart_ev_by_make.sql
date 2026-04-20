with base as (
    select
        make,
        ev_category,
        registrations
    from {{ ref('int_registrations_enriched') }}
)

select
    make,

    -- total EV registrations (BEV + PHEV + HEV)
    sum(case when ev_category in ('BEV', 'PHEV', 'HEV') then registrations else 0 end) as ev_registrations,

    -- optional breakdowns
    sum(case when ev_category = 'BEV' then registrations else 0 end) as bev_registrations,
    sum(case when ev_category = 'PHEV' then registrations else 0 end) as phev_registrations,
    sum(case when ev_category = 'HEV' then registrations else 0 end) as hev_registrations,
    sum(case when ev_category = 'ICE' then registrations else 0 end) as ice_registrations,

    -- total registrations for that make
    sum(registrations) as total_registrations

from base
group by make
order by ev_registrations desc
