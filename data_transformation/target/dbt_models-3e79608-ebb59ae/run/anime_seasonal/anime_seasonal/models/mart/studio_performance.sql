
  create view "anime_seasonal"."mart"."studio_performance__dbt_tmp"
    
    
  as (
    
SELECT st.studios,
    count(distinct f.anime_id) AS total_anime,
    round(avg(f.score), 2) AS avg_score,
    sum(f.members) AS total_members,
    sum(f.favorites) AS total_favorites
FROM "anime_seasonal"."gold"."fact_anime" f
    JOIN "anime_seasonal"."gold"."dim_studios" st ON f.anime_id = st.anime_id
GROUP BY st.studios
  );