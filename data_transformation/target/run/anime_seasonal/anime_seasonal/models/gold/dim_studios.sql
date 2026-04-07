
      
  
    

  create  table "anime_seasonal"."gold"."dim_studios__dbt_tmp"
  
  
    as
  
  (
    




WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)
SELECT
    anime_id,

    
        studios
    

FROM source_data
  );
  
  