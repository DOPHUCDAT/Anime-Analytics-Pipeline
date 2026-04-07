
      
        
        
        delete from "anime_seasonal"."gold"."dim_studios" as DBT_INTERNAL_DEST
        where (anime_id) in (
            select distinct anime_id
            from "dim_studios__dbt_tmp171747485781" as DBT_INTERNAL_SOURCE
        );

    

    insert into "anime_seasonal"."gold"."dim_studios" ("anime_id", "studios")
    (
        select "anime_id", "studios"
        from "dim_studios__dbt_tmp171747485781"
    )
  