
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "anime_seasonal"."gold"."dim_airing"

where not(aired_to is null or aired_to >= aired_from)


  
  
      
    ) dbt_internal_test