




WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)

SELECT
    anime_id,
    title,
    type,
    source,
    status,
    rating,

    
        genres
    

FROM source_data