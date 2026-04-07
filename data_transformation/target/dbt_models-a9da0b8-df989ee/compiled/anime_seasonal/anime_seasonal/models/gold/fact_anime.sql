

WITH source_data AS (
    SELECT *
    FROM "anime_seasonal"."silver"."cleaned_data"
),

max_date AS (
    SELECT COALESCE(MAX(aired_from), '1900-01-01') AS max_aired_from
    FROM "anime_seasonal"."gold"."fact_anime"
)

SELECT
    anime_id,
    score,
    scored_by,
    popularity,
    members,
    favorites,
    ranking,
    episodes,
    aired_from
FROM source_data


WHERE aired_from > (SELECT max_aired_from FROM max_date)
