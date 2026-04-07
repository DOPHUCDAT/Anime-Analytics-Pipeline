
      
  
    

  create  table "anime_seasonal"."gold"."dim_season__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)

SELECT DISTINCT
    anime_id,
    season,
    year
FROM source_data


  );
  
  