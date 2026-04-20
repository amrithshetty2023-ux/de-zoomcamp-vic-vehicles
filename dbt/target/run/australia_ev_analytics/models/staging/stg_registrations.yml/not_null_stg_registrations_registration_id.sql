
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select registration_id
from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`stg_registrations`
where registration_id is null



  
  
      
    ) dbt_internal_test