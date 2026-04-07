

WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)

SELECT DISTINCT
    anime_id,
    season,
    year
FROM source_data

