{{ config(
    materialized = 'incremental',
    unique_key = 'anime_id',
    schema = "silver",
    order_by = "anime_id"
) }} WITH anime_sk AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY anime_id, aired_from) AS rn
    FROM {{ source('ingest_layer', 'raw_anime') }}
    WHERE anime_id IS NOT NULL
)
SELECT {{ dbt_utils.generate_surrogate_key(['anime_id', 'aired_from']) }} AS anime_id,
    TRIM(title) AS title,
    type,
    source,
    genres,
    TRIM(rating) AS rating,
    CASE
        WHEN episodes < 0 THEN ABS(episodes)
        ELSE episodes
    END::INT AS episodes,
    CASE
        WHEN score < 0 THEN ABS(score)
        ELSE score
    END::NUMERIC(4, 3) AS score,
    CASE
        WHEN scored_by < 0 THEN ABS(scored_by)
        ELSE scored_by
    END::INT AS scored_by,
    CASE
        WHEN popularity < 0 THEN ABS(popularity)
        ELSE popularity
    END::INT AS popularity,
    CASE
        WHEN members < 0 THEN ABS(members)
        ELSE members
    END::INT AS members,
    CASE
        WHEN favorites < 0 THEN ABS(favorites)
        ELSE favorites
    END::INT AS favorites,
    CASE
        WHEN rank < 0 THEN ABS(rank)
        ELSE rank
    END::INT AS ranking,
    TRIM(season) AS season,
    CAST(year AS INT) AS year,
    TRIM(status) AS status,
    CASE
        WHEN aired_to IS NOT NULL
        AND aired_from IS NOT NULL
        AND aired_to < aired_from THEN aired_to::date
        ELSE aired_from::date
    END AS aired_from,
    CASE
        WHEN aired_to IS NOT NULL
        AND aired_from IS NOT NULL
        AND aired_to < aired_from THEN aired_from::date
        ELSE aired_to::date
    END AS aired_to,
    TRIM(studios) AS studios
FROM anime_sk
WHERE rn = 1