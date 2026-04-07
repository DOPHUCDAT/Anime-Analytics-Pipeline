




WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
),

max_date AS (
    SELECT COALESCE(MAX(aired_from), '1900-01-01') AS max_aired_from
    FROM "anime_seasonal"."gold"."dim_studios"
)

SELECT
    anime_id,

    
        studios
    ,

    aired_from

FROM spurce_data


WHERE aired_from > (SELECT max_aired_from FROM max_date)
