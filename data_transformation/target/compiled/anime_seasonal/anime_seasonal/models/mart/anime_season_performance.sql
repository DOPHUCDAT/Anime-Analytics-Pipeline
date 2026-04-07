
SELECT s.year,
    s.season,
    count(DISTINCT f.anime_id) AS total_anime,
    round(avg(f.score), 2) AS avg_score,
    sum(f.members) AS total_members,
    sum(f.favorites) AS total_favorites
FROM "anime_seasonal"."gold"."fact_anime" f
    JOIN "anime_seasonal"."gold"."dim_season" s ON f.anime_id = s.anime_id
GROUP BY s.year,
    s.season