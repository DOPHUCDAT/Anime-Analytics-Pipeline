import time
from datetime import datetime
from typing import Any, Dict, Iterable, List

import dlt
import requests

BASE_URL = "https://api.jikan.moe/v4"
SEASON_ENDPOINT = "seasons/{year}/{season}"
SEASONS = ["winter", "spring", "summer", "fall"]
REQUEST_DELAY_SECONDS = 0.5
START_YEAR = 2005


class JikanSeasonalClient:
    def __init__(self, base_url: str = BASE_URL, endpoint: str = SEASON_ENDPOINT):
        self.base_url = base_url
        self.endpoint = endpoint

    def get_last_page(self, year: int, season: str) -> int:
        response_json = self._fetch_page(year, season, 1)
        return response_json.get("pagination", {}).get("last_visible_page", 1)

    def get_page_data(self, year: int, season: str, page: int) -> List[Dict[str, Any]]:
        response_json = self._fetch_page(year, season, page)
        return response_json.get("data", [])

    def _fetch_page(self, year: int, season: str, page: int) -> Dict[str, Any]:
        url = f"{self.base_url}/{self.endpoint.format(year=year, season=season)}?page={page}"
        return requests.get(url).json()


class AnimeRecordMapper:
    @staticmethod
    def map_record(anime: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "anime_id": anime.get("mal_id"),
            "title": anime.get("title"),
            "type": anime.get("type"),
            "source": anime.get("source"),
            "rating": anime.get("rating"),
            "score": anime.get("score"),
            "scored_by": anime.get("scored_by"),
            "popularity": anime.get("popularity"),
            "members": anime.get("members"),
            "favorites": anime.get("favorites"),
            "rank": anime.get("rank"),
            "episodes": anime.get("episodes"),
            "genres": ", ".join([g["name"] for g in anime.get("genres", [])]),
            "season": anime.get("season"),
            "year": anime.get("year"),
            "status": anime.get("status"),
            "aired_from": anime.get("aired", {}).get("from"),
            "aired_to": anime.get("aired", {}).get("to"),
            "studios": ", ".join([s.get("name") for s in anime.get("studios", [])]),
        }


@dlt.resource(
    name="raw_anime",
    write_disposition="merge",
    primary_key="anime_id",
)
def fetch_all_anime():

    state = dlt.current.resource_state()
    current_year = datetime.now().year

    is_first_run = not state.get("initialized", False)

    client = JikanSeasonalClient()
    mapper = AnimeRecordMapper()

    total_records_all = 0

    if is_first_run:
        print("\n=== INITIAL RUN ===")
        years = range(START_YEAR, current_year + 1)
        state["initialized"] = True
    else:
        print("\n=== INCREMENTAL RUN ===")
        years = [current_year]

    for year in years:
        year_total = 0

        for season in SEASONS:
            print(f"\n=== Fetching {season.upper()} {year} ===")

            season_total = 0
            last_page = client.get_last_page(year, season)

            print(f"[{season} {year}] Total pages: {last_page}")

            for page in range(1, last_page + 1):
                data = client.get_page_data(year, season, page)
                page_count = len(data)

                season_total += page_count
                year_total += page_count
                total_records_all += page_count

                print(f"[{season} {year}] Page {page}/{last_page} → {page_count} records")

                for anime in data:
                    yield mapper.map_record(anime)

                time.sleep(REQUEST_DELAY_SECONDS)

            print(f" DONE {season} {year} -> {season_total} records")


    state["last_run"] = str(datetime.now())


@dlt.source()
def anime_source():
    return fetch_all_anime()