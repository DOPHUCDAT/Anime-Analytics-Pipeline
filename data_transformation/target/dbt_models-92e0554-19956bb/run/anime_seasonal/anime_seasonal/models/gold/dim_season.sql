
      
        delete from "anime_seasonal"."gold"."dim_season" as DBT_INTERNAL_DEST
        where (anime_id, season, year) in (
            select distinct anime_id, season, year
            from "dim_season__dbt_tmp113136655821" as DBT_INTERNAL_SOURCE
        );

    

    insert into "anime_seasonal"."gold"."dim_season" ("anime_id", "season", "year")
    (
        select "anime_id", "season", "year"
        from "dim_season__dbt_tmp113136655821"
    )
  