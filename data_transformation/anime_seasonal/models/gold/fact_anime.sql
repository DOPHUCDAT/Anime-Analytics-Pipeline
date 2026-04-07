{{ config(
    schema='gold',
    materialized='incremental',
    unique_key='anime_id'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('cleaned_data') }}
)
SELECT
    anime_id,
    score,
    scored_by,
    popularity,
    members,
    favorites,
    ranking,
    episodes
FROM source_data
