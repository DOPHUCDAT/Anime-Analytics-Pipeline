
      
        
        
        delete from "anime_seasonal"."gold"."dim_airing" as DBT_INTERNAL_DEST
        where (anime_id) in (
            select distinct anime_id
            from "dim_airing__dbt_tmp112816142555" as DBT_INTERNAL_SOURCE
        );

    

    insert into "anime_seasonal"."gold"."dim_airing" ("anime_id", "aired_from", "aired_to")
    (
        select "anime_id", "aired_from", "aired_to"
        from "dim_airing__dbt_tmp112816142555"
    )
  