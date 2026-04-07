

WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
)

SELECT DISTINCT
    anime_id,
    season,
    year
FROM source_data


WHERE (anime_id, season, year) NOT IN (
    SELECT anime_id, season, year FROM "anime_seasonal"."gold"."dim_season"
)
