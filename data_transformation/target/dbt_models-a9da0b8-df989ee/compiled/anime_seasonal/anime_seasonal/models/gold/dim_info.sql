




WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
),

max_date AS (
    SELECT COALESCE(MAX(aired_from), '1900-01-01') AS max_aired_from
    FROM "anime_seasonal"."gold"."dim_info"
)

SELECT
    anime_id,
    title,
    type,
    source,
    status,
    rating,

    
        genres
    

FROM "anime_seasonal"."silver"."cleaned_data"


WHERE aired_from > (SELECT max_aired_from FROM max_date)
