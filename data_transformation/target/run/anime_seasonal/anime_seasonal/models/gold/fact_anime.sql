
      
  
    

  create  table "anime_seasonal"."gold"."fact_anime__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)
SELECT
    anime_id,
    score,
    scored_by,
    popularity,
    members,
    favorites,
    ranking,
    episodes
FROM source_data
  );
  
  