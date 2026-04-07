
      
        
        
        delete from "anime_seasonal"."gold"."dim_info" as DBT_INTERNAL_DEST
        where (anime_id) in (
            select distinct anime_id
            from "dim_info__dbt_tmp171747462908" as DBT_INTERNAL_SOURCE
        );

    

    insert into "anime_seasonal"."gold"."dim_info" ("anime_id", "title", "type", "source", "status", "rating", "genres")
    (
        select "anime_id", "title", "type", "source", "status", "rating", "genres"
        from "dim_info__dbt_tmp171747462908"
    )
  