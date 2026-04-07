
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select aired_from
from "anime_seasonal"."gold"."dim_airing"
where aired_from is null



  
  
      
    ) dbt_internal_test