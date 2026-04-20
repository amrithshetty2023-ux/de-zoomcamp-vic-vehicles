
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select model_standardized
from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`int_registrations_enriched`
where model_standardized is null



  
  
      
    ) dbt_internal_test