import time
from datetime import datetime

import dlt
import requests


@dlt.resource(
    name="raw_anime",
    write_disposition={"disposition": "merge", "strategy": "scd2"},
    primary_key="anime_id",
)
def fetch_all_anime():
    base_url = "https://api.jikan.moe/v4"
    endpoint = "seasons/{year}/{season}"
    seasons = ["winter", "spring", "summer", "fall"]
    current_year = datetime.now().year

    for year in range(2005, current_year + 1):
        for season in seasons:
            print(f"\n=== Fetching {season} {year} ===")

            url_first_page = (
                f"{base_url}/{endpoint.format(year=year, season=season)}?page=1"
            )
            res = requests.get(url_first_page).json()

            pagination = res.get("pagination", {})
            last_page = pagination.get("last_visible_page", 1)

            print(f"[{season} {year}] Total pages: {last_page}")

            total_records = 0

            for page in range(1, last_page + 1):
                print(f"[{season} {year}] Processing page {page}/{last_page}")
                url = f"{base_url}/{endpoint.format(year=year, season=season)}?page={page}"

                resp = requests.get(url).json()
                data = resp.get("data", [])

                total_records += len(data)

                for anime in data:
                    yield {
                        "anime_id": anime.get("mal_id"),
                        "title": anime.get("title"),
                        "type": anime.get("type"),
                        "source": anime.get("source"),
                        "score": anime.get("score"),
                        "scored_by": anime.get("scored_by"),
                        "popularity": anime.get("popularity"),
                        "members": anime.get("members"),
                        "favorites": anime.get("favorites"),
                        "rank": anime.get("rank"),
                        "episodes": anime.get("episodes"),
                        "season": anime.get("season"),
                        "year": anime.get("year"),
                        "status": anime.get("status"),
                        "aired_from": anime.get("aired", {}).get("from"),
                        "aired_to": anime.get("aired", {}).get("to"),
                        "studios": ", ".join(
                            [s.get("name") for s in anime.get("studios", [])]
                        ),
                    }
                time.sleep(0.5)

            print(f" DONE {season} {year}: {total_records} records")


@dlt.source()
def anime_source():
    return fetch_all_anime()


# def main():
#     pipeline = dlt.pipeline(
#         pipeline_name="anime_list_pipeline",
#         destination="postgres",
#         dataset_name="anime_data",
#     )
#     pipeline.run(anime_source())


# if __name__ == "__main__":
#     main()
