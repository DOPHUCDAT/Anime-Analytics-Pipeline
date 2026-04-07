{{ config(
    materialized = 'incremental',
    schema = 'gold',
    unique_key = 'anime_id',
    on_schema_change = 'append_new_columns'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('cleaned_data') }}
)
SELECT
    anime_id,
    studios
FROM source_data

