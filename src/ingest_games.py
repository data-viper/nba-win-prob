import os
import time
import random
import requests
import pandas as pd
from datetime import date, timedelta

BASE_V1 = "https://api.balldontlie.io/nba/v1"

def api_key() -> str:
    key = os.getenv("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY is not set.")
    return key

def get_with_retry(url: str, headers: dict, params: dict, timeout: int = 30, max_retries: int = 8):
    """
    Handles rate limits (429) with exponential backoff + jitter.
    """
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, headers=headers, params=params, timeout=timeout)

        if r.status_code != 429:
            r.raise_for_status()
            return r

        # If rate-limited, wait and retry
        retry_after = r.headers.get("Retry-After")
        if retry_after:
            wait_s = float(retry_after)
        else:
            wait_s = delay + random.uniform(0, 1.0)

        print(f"[429] Rate limited. Waiting {wait_s:.1f}s (attempt {attempt}/{max_retries})...")
        time.sleep(wait_s)
        delay = min(delay * 2, 60)  # cap wait

    raise RuntimeError("Hit rate limit too many times. Try again later or reduce date range.")

def fetch_games_for_date(d: str) -> list[dict]:
    headers = {"Authorization": api_key()}
    url = f"{BASE_V1}/games"
    params = {"dates[]": d, "per_page": 100}

    rows = []
    page = 1
    while True:
        params["page"] = page
        r = get_with_retry(url, headers, params)
        payload = r.json()
        rows.extend(payload.get("data", []))

        meta = payload.get("meta", {})
        next_page = meta.get("next_page")
        if not next_page:
            break
        page = next_page

        # be gentle; free tier is ~5 req/min
        time.sleep(1.0)

    # also pause between dates
    time.sleep(1.0)
    return rows

def main():
    # Start small to avoid rate limit
    days_back = int(os.getenv("DAYS_BACK", "14"))
    end = date.today()
    start = end - timedelta(days=days_back)

    all_rows = []
    cur = start
    while cur <= end:
        print(f"Fetching games for {cur.isoformat()} ...")
        all_rows.extend(fetch_games_for_date(cur.isoformat()))
        cur += timedelta(days=1)

    if not all_rows:
        print("No games returned from API.")
        return

    df = pd.json_normalize(all_rows)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    df_out = df[[
        "id", "date", "season", "status",
        "home_team.id", "home_team.full_name", "home_team.abbreviation",
        "visitor_team.id", "visitor_team.full_name", "visitor_team.abbreviation",
        "home_team_score", "visitor_team_score"
    ]].copy()

    df_out.columns = [
        "game_id", "game_date_utc", "season", "status",
        "home_team_id", "home_team_name", "home_team_abbr",
        "away_team_id", "away_team_name", "away_team_abbr",
        "home_score", "away_score"
    ]

    os.makedirs("data", exist_ok=True)
    out_path = "data/games_recent.parquet"
    df_out.to_parquet(out_path, index=False)
    print(f"Saved {len(df_out):,} rows to {out_path}")

if __name__ == "__main__":
    main()

