
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select type
from "anime_seasonal"."gold"."dim_info"
where type is null



  
  
      
    ) dbt_internal_test