
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    anime_id as unique_field,
    count(*) as n_records

from "anime_seasonal"."gold"."dim_info"
where anime_id is not null
group by anime_id
having count(*) > 1



  
  
      
    ) dbt_internal_test