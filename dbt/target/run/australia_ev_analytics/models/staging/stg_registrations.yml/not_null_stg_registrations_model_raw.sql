
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select model_raw
from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`stg_registrations`
where model_raw is null



  
  
      
    ) dbt_internal_test