
      
  
    

  create  table "anime_seasonal"."gold"."dim_airing__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)
SELECT
    anime_id,
    aired_from,
    aired_to
FROM source_data
  );
  
  