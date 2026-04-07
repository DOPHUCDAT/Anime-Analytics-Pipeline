
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with meet_condition as(
  select *
  from "anime_seasonal"."gold"."dim_season"
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not year >= 1900
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not year <= 2100
)

select *
from validation_errors


  
  
      
    ) dbt_internal_test