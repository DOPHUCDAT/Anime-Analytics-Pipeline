
      
        
        
        delete from "anime_seasonal"."gold"."fact_anime" as DBT_INTERNAL_DEST
        where (anime_id) in (
            select distinct anime_id
            from "fact_anime__dbt_tmp112816224722" as DBT_INTERNAL_SOURCE
        );

    

    insert into "anime_seasonal"."gold"."fact_anime" ("anime_id", "score", "scored_by", "popularity", "members", "favorites", "ranking", "episodes")
    (
        select "anime_id", "score", "scored_by", "popularity", "members", "favorites", "ranking", "episodes"
        from "fact_anime__dbt_tmp112816224722"
    )
  