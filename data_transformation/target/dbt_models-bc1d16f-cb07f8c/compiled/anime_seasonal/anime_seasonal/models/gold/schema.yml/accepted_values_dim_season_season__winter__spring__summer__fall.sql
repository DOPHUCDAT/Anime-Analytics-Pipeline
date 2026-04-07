
    
    

with all_values as (

    select
        season as value_field,
        count(*) as n_records

    from "anime_seasonal"."gold"."dim_season"
    group by season

)

select *
from all_values
where value_field not in (
    'winter','spring','summer','fall'
)


