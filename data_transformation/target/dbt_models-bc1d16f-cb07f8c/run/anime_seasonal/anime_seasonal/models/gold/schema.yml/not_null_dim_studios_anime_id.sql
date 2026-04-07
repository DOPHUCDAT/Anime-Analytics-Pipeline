
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select anime_id
from "anime_seasonal"."gold"."dim_studios"
where anime_id is null



  
  
      
    ) dbt_internal_test