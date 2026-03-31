# scripts/refresh_odds.py
# Lightweight script — only refreshes odds for today's daily CSV
# Runs at 11 AM ET and 2 PM ET to catch late-posting Pinnacle lines

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

API_KEY = os.environ.get("API_SPORTS_KEY")
if not API_KEY:
    raise ValueError("API_SPORTS_KEY environment variable not set.")

HEADERS = {"x-apisports-key": API_KEY}
TARGET_ODDS = 1.909
eastern = pytz.timezone("US/Eastern")

# Check both today ET and yesterday ET to handle UTC/ET boundary
now_et = datetime.now(eastern)
candidate_dates = [
    now_et.strftime("%Y-%m-%d"),
    (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
]

print(f"🔄 Odds refresh running | UTC: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} | ET: {now_et.strftime('%Y-%m-%d %H:%M')}")

# Find the most recent daily file that exists
filename = None
today = None
for date in candidate_dates:
    candidate = f"data/daily/MLB_Combined_Odds_Results_{date}.csv"
    if os.path.exists(candidate):
        filename = candidate
        today = date
        print(f"📁 Found daily file: {candidate}")
        break

if not filename:
    print(f"⚠️ No daily file found for {candidate_dates} — skipping")
    exit(0)

df = pd.read_csv(filename)

# Find games with missing moneyline OR total
missing = df[df['moneyline_home'].isna() | df['total_line'].isna()]

if len(missing) == 0:
    print(f"✅ All odds present for {today} — nothing to refresh")
    exit(0)

print(f"🔄 Found {len(missing)} games with missing odds — refreshing...")

for idx, row in missing.iterrows():
    game_id = int(row['game_id'])
    try:
        url = f"https://v1.baseball.api-sports.io/odds?game={game_id}&bookmaker=4"
        response = requests.get(url, headers=HEADERS, timeout=10)
        data = response.json()

        if not data.get("response"):
            print(f"  ⚠️ {row['home_team']} vs {row['away_team']}: still no odds available")
            continue

        bookmakers = data["response"][0].get("bookmakers", [])
        if not bookmakers:
            print(f"  ⚠️ {row['home_team']} vs {row['away_team']}: no bookmakers returned")
            continue

        bets = bookmakers[0].get("bets", [])

        for bet in bets:
            if bet["name"] == "Home/Away":
                for val in bet.get("values", []):
                    if val["value"].lower() == "home":
                        df.at[idx, "moneyline_home"] = val["odd"]
                    elif val["value"].lower() == "away":
                        df.at[idx, "moneyline_away"] = val["odd"]

            elif bet["name"] == "Over/Under":
                totals = {}
                for val in bet.get("values", []):
                    try:
                        parts = val["value"].split(" ")
                        side = parts[0].lower()
                        line = float(parts[1])
                        if line not in totals:
                            totals[line] = {}
                        totals[line][side] = float(val["odd"])
                    except (IndexError, ValueError):
                        continue

                valid = [(l, s) for l, s in totals.items() if "over" in s and "under" in s]
                if valid:
                    best_line = min(
                        valid,
                        key=lambda x: (abs(x[1]["over"] - TARGET_ODDS) + abs(x[1]["under"] - TARGET_ODDS)) / 2
                    )[0]
                    df.at[idx, "total_line"] = best_line
                    df.at[idx, "over_odds"] = totals[best_line]["over"]
                    df.at[idx, "under_odds"] = totals[best_line]["under"]

        print(f"  ✅ {row['home_team']} vs {row['away_team']}: "
              f"ML={df.at[idx, 'moneyline_home']} Total={df.at[idx, 'total_line']}")

    except Exception as e:
        print(f"  ❌ Error for game {game_id}: {e}")

df.to_csv(filename, index=False)
print(f"\n✅ Odds refresh complete — {today} updated")
