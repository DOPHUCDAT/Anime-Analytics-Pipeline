
      insert into "anime_seasonal"."silver"."cleaned_data" ("anime_id", "title", "type", "source", "genres", "rating", "episodes", "score", "scored_by", "popularity", "members", "favorites", "ranking", "season", "year", "status", "aired_from", "aired_to", "studios")
    (
        select "anime_id", "title", "type", "source", "genres", "rating", "episodes", "score", "scored_by", "popularity", "members", "favorites", "ranking", "season", "year", "status", "aired_from", "aired_to", "studios"
        from "cleaned_data__dbt_tmp111726523401"
    )


  