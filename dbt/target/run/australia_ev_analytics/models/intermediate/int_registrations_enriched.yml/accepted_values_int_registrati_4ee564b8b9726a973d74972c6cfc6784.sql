
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        ev_category as value_field,
        count(*) as n_records

    from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`int_registrations_enriched`
    group by ev_category

)

select *
from all_values
where value_field not in (
    'BEV','PHEV','HEV','ICE'
)



  
  
      
    ) dbt_internal_test