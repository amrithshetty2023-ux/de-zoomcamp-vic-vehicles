
    
    

with dbt_test__target as (

  select registration_id as unique_field
  from `dtc-de-course-capstone-2026`.`vic_vehicle_analytics`.`stg_registrations`
  where registration_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


