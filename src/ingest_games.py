import os
import time
import random
import requests
import pandas as pd

# balldontlie v1 endpoint for games (cursor pagination)
GAMES_URL = "https://api.balldontlie.io/v1/games"


def api_key() -> str:
    key = os.getenv("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY is not set.")
    return key


def get_with_retry(
    url: str,
    headers: dict,
    params: dict,
    timeout: int = 30,
    max_retries: int = 10,
):
    """
    GET with retry/backoff for rate limits (429) and transient errors.
    """
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as e:
            # transient network error
            wait_s = min(delay + random.uniform(0, 1.0), 60)
            print(f"[network] {e}. Waiting {wait_s:.1f}s (attempt {attempt}/{max_retries})...")
            time.sleep(wait_s)
            delay = min(delay * 2, 60)
            continue

        # Rate limit
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after else min(delay + random.uniform(0, 1.0), 60)
            print(f"[429] Rate limited. Waiting {wait_s:.1f}s (attempt {attempt}/{max_retries})...")
            time.sleep(wait_s)
            delay = min(delay * 2, 60)
            continue

        # Other HTTP errors
        if r.status_code >= 400:
            # Surface useful details
            try:
                msg = r.json()
            except Exception:
                msg = r.text[:500]
            raise RuntimeError(f"HTTP {r.status_code} error for {r.url}\nResponse: {msg}")

        return r

    raise RuntimeError("Too many retries. Try again later or slow the request pace.")


def fetch_games_for_season(season: int, sleep_s: float = 0.25) -> list[dict]:
    """
    Fetch all games for a season using cursor-based pagination.
    """
    headers = {"Authorization": api_key()}
    params = {"seasons[]": season, "per_page": 100}

    rows: list[dict] = []
    cursor = None
    page_count = 0

    while True:
        if cursor is not None:
            params["cursor"] = cursor
        else:
            params.pop("cursor", None)

        r = get_with_retry(GAMES_URL, headers, params)
        payload = r.json()

        batch = payload.get("data", [])
        rows.extend(batch)

        meta = payload.get("meta", {}) or {}
        cursor = meta.get("next_cursor")

        page_count += 1
        print(f"  season {season} | page {page_count} | fetched {len(batch)} | total {len(rows)}")

        if not cursor:
            break

        time.sleep(sleep_s)

    return rows


def main():
    seasons_str = os.getenv("SEASONS", "").strip()
    if not seasons_str:
        raise RuntimeError('Set SEASONS, e.g. $env:SEASONS="2023,2024"')

    seasons = [int(s.strip()) for s in seasons_str.split(",") if s.strip()]

    all_rows: list[dict] = []
    for s in seasons:
        print(f"Fetching season {s} ...")
        all_rows.extend(fetch_games_for_season(s))

    if not all_rows:
        print("No games returned.")
        return

    df = pd.json_normalize(all_rows)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    # Select/rename the columns we care about
    df_out = df[
        [
            "id",
            "date",
            "season",
            "status",
            "home_team.id",
            "home_team.full_name",
            "home_team.abbreviation",
            "visitor_team.id",
            "visitor_team.full_name",
            "visitor_team.abbreviation",
            "home_team_score",
            "visitor_team_score",
        ]
    ].copy()

    df_out.columns = [
        "game_id",
        "game_date_utc",
        "season",
        "status",
        "home_team_id",
        "home_team_name",
        "home_team_abbr",
        "away_team_id",
        "away_team_name",
        "away_team_abbr",
        "home_score",
        "away_score",
    ]

    os.makedirs("data", exist_ok=True)
    out_path = f"data/games_seasons_{'_'.join(map(str, seasons))}.parquet"
    df_out.to_parquet(out_path, index=False)
    print(f"\nSaved {len(df_out):,} rows to {out_path}")


if __name__ == "__main__":
    main()

