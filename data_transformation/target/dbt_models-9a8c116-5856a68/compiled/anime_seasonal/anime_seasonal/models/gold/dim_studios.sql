




SELECT
    anime_id,

    
        studios
    ,

    aired_from

FROM "anime_seasonal"."silver"."cleaned_data"


WHERE aired_from > (
    SELECT COALESCE(MAX(aired_from), '1900-01-01')
    FROM "anime_seasonal"."gold"."dim_studios"
)
