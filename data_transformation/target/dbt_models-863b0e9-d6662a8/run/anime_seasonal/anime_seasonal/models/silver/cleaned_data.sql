
      
        
        
        delete from "anime_seasonal"."silver"."cleaned_data" as DBT_INTERNAL_DEST
        where (anime_id) in (
            select distinct anime_id
            from "cleaned_data__dbt_tmp113848467912" as DBT_INTERNAL_SOURCE
        );

    

    insert into "anime_seasonal"."silver"."cleaned_data" ("anime_id", "title", "type", "source", "genres", "rating", "episodes", "score", "scored_by", "popularity", "members", "favorites", "ranking", "season", "year", "status", "aired_from", "aired_to", "studios")
    (
        select "anime_id", "title", "type", "source", "genres", "rating", "episodes", "score", "scored_by", "popularity", "members", "favorites", "ranking", "season", "year", "status", "aired_from", "aired_to", "studios"
        from "cleaned_data__dbt_tmp113848467912"
    )
  