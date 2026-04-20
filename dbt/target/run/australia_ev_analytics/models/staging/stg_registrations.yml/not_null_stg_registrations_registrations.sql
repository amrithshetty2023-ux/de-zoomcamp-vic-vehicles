
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select registrations
from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`stg_registrations`
where registrations is null



  
  
      
    ) dbt_internal_test