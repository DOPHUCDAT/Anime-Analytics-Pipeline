{{ config(
    materialized = 'incremental',
    schema = 'gold',
    unique_key = ['anime_id', 'season', 'year'],
    on_schema_change = 'append_new_columns'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('cleaned_data') }}
)

SELECT DISTINCT
    anime_id,
    season,
    year
FROM source_data

{% if is_incremental() %}
WHERE (anime_id, season, year) NOT IN (
    SELECT anime_id, season, year FROM {{ this }}
)
{% endif %}