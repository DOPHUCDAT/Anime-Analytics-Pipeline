



select
    1
from "anime_seasonal"."gold"."dim_airing"

where not(aired_to is null or aired_to >= aired_from)

