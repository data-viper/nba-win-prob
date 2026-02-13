@'
import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone, date

BASE_V1 = "https://api.balldontlie.io/nba/v1"

def api_key() -> str:
    key = os.getenv("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY is not set. Set it in PowerShell or .env.")
    return key

def fetch_games_for_date(d: str) -> pd.DataFrame:
    headers = {"Authorization": api_key()}
    url = f"{BASE_V1}/games"
    params = {"dates[]": d, "per_page": 100}

    rows = []
    page = 1
    while True:
        params["page"] = page
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        rows.extend(payload.get("data", []))

        meta = payload.get("meta", {})
        next_page = meta.get("next_page")
        if not next_page:
            break
        page = next_page
        time.sleep(0.4)

    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df

def main():
    d = date.today().isoformat()
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    df = fetch_games_for_date(d)

    os.makedirs("predictions", exist_ok=True)

    if df.empty:
        out = pd.DataFrame([{
            "created_at_utc": created_at,
            "note": "No games scheduled for this date via API"
        }])
        out_path = f"predictions/{d}.csv"
        out.to_csv(out_path, index=False)
        print(f"Wrote {out_path}")
        return

    out = pd.DataFrame({
        "game_id": df["id"],
        "game_date_utc": df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "home_team": df["home_team.full_name"],
        "away_team": df["visitor_team.full_name"],
        "p_home_win": 0.55,
        "model_version": "v0_home_advantage",
        "created_at_utc": created_at
    })

    out_path = f"predictions/{d}.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(out)} games")

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 .\src\predict_today.py
