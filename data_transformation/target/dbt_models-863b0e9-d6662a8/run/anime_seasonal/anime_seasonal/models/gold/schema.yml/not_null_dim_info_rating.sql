
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rating
from "anime_seasonal"."gold"."dim_info"
where rating is null



  
  
      
    ) dbt_internal_test