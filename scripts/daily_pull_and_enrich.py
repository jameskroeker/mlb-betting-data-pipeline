# scripts/daily_pull_and_enrich.py

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

# === Config ===
API_KEY = os.environ.get("API_SPORTS_KEY")
if not API_KEY:
    raise ValueError("API_SPORTS_KEY environment variable not set. Please add it as a GitHub Secret.")

HEADERS = {"x-apisports-key": API_KEY}

# === CHANGED: Dynamically set season based on current year ===
CURRENT_SEASON = datetime.now().year

utc = pytz.utc
eastern = pytz.timezone("US/Eastern")

os.makedirs("data/daily", exist_ok=True)

# === Team Name Normalization ===
TEAM_NAME_FIXES = {
    "St.Louis Cardinals": "St. Louis Cardinals"
}

def normalize_team_name(name):
    return TEAM_NAME_FIXES.get(name, name)

# === Utility Functions ===
def safe_inning_scores(scores_dict):
    return scores_dict.get("innings", {}) if scores_dict else {}

def enrich_results_for_games(games):
    print(f"Attempting to enrich {len(games)} games...")
    enriched_count = 0
    for game_id, game in games.items():
        try:
            url = f"https://v1.baseball.api-sports.io/games?id={game_id}"
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data or not data.get("response"):
                continue

            g = data["response"][0]

            if g["status"]["long"] != "Finished":
                continue

            scores = g.get("scores", {})
            game["status"] = g["status"]["long"]
            game["home_score"] = scores.get("home", {}).get("total")
            game["away_score"] = scores.get("away", {}).get("total")

            if game["home_score"] is not None and game["away_score"] is not None:
                if game["home_score"] > game["away_score"]:
                    game["winner"] = game["home_team"]
                elif game["home_score"] < game["away_score"]:
                    game["winner"] = game["away_team"]
                else:
                    game["winner"] = "Draw"

                if game["total_line"] is not None:
                    total = game["home_score"] + game["away_score"]
                    game["total_result"] = "Over" if total > game["total_line"] else "Under"
                else:
                    game["total_result"] = None

            home_innings = safe_inning_scores(scores.get("home"))
            away_innings = safe_inning_scores(scores.get("away"))
            for i in range(1, 10):
                game[f"home_{i}"] = home_innings.get(str(i))
                game[f"away_{i}"] = away_innings.get(str(i))
            enriched_count += 1
        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error enriching game {game_id}: {e}")
        except Exception as e:
            print(f"⚠️ Error enriching game {game_id}: {e}")
    print(f"Finished enriching. Successfully enriched {enriched_count} games.")

def pull_games_and_odds(target_date):
    print(f"\n📅 Pulling game schedule and odds for {target_date}")
    api_dates = [target_date, (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")]
    games = {}

    for api_date in api_dates:
        # === CHANGED: Use CURRENT_SEASON variable instead of hardcoded 2025 ===
        url = f"https://v1.baseball.api-sports.io/games?league=1&season={CURRENT_SEASON}&date={api_date}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data or not data.get("response"):
                print(f"⚠️ No API response for date {api_date} or empty response.")
                continue

            for g in data.get("response", []):
                try:
                    game_id = g["id"]
                    utc_start = datetime.fromisoformat(g["date"].replace("Z", "+00:00"))
                    et_start = utc_start.astimezone(eastern)

                    if et_start.strftime("%Y-%m-%d") != target_date:
                        continue

                    game_data = {
                        "game_id": game_id,
                        "game_date": et_start.strftime("%Y-%m-%d"),
                        "start_time_et": et_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "home_team": normalize_team_name(g["teams"]["home"]["name"]),
                        "away_team": normalize_team_name(g["teams"]["away"]["name"]),
                        "moneyline_home": None, "moneyline_away": None,
                        "total_line": None, "over_odds": None, "under_odds": None,
                        "home_score": None, "away_score": None,
                        "status": None, "winner": None, "total_result": None,
                    }
                    for i in range(1, 10):
                        game_data[f"home_{i}"] = None
                        game_data[f"away_{i}"] = None
                    games[game_id] = game_data

                except Exception as e:
                    print(f"⚠️ Error processing game metadata for date {api_date} (Game ID: {g.get('id', 'N/A')}): {e}")
        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error fetching games for date {api_date}: {e}")
        except Exception as e:
            print(f"❌ An unexpected error occurred fetching games for date {api_date}: {e}")

    # Pull Odds
    # CHANGED: Pinnacle (ID 4) replaces Betway (ID 3) — Betway has no baseball markets
    # CHANGED: Consensus total logic — finds Over/Under pair closest to 1.909 (-110 equivalent)
    TARGET_ODDS = 1.909
    for game_id, game in games.items():
        try:
            odds_url = f"https://v1.baseball.api-sports.io/odds?game={game_id}&bookmaker=4"
            response = requests.get(odds_url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            odds_data = response.json()

            if not odds_data or not odds_data.get("response"):
                continue

            bookmakers_data = odds_data["response"][0].get("bookmakers")
            if not bookmakers_data:
                continue

            bets = bookmakers_data[0].get("bets")
            if not bets:
                continue

            for bet in bets:
                if bet["name"] == "Home/Away":
                    for val in bet.get("values", []):
                        opt = val["value"].lower()
                        if opt == "home":
                            game["moneyline_home"] = val["odd"]
                        elif opt == "away":
                            game["moneyline_away"] = val["odd"]

                elif bet["name"] == "Over/Under":
                    # Build a dict of {line: {over: odd, under: odd}}
                    totals_by_line = {}
                    for val in bet.get("values", []):
                        try:
                            parts = val["value"].split(" ")
                            side = parts[0].lower()   # "over" or "under"
                            line = float(parts[1])    # e.g. 7.5
                        except (IndexError, ValueError):
                            continue
                        if line not in totals_by_line:
                            totals_by_line[line] = {}
                        totals_by_line[line][side] = float(val["odd"])

                    # Find the line where both sides are closest to -110 (1.909 decimal)
                    best_line = None
                    best_distance = float("inf")
                    for line, sides in totals_by_line.items():
                        if "over" in sides and "under" in sides:
                            avg_dist = (abs(sides["over"] - TARGET_ODDS) + abs(sides["under"] - TARGET_ODDS)) / 2
                            if avg_dist < best_distance:
                                best_distance = avg_dist
                                best_line = line

                    if best_line is not None:
                        game["total_line"] = best_line
                        game["over_odds"] = totals_by_line[best_line].get("over")
                        game["under_odds"] = totals_by_line[best_line].get("under")

        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error fetching odds for game {game_id}: {e}")
        except Exception as e:
            print(f"⚠️ Error fetching or processing odds for game {game_id}: {e}")
            continue

    return games

# =========================================================================
# === MAIN EXECUTION LOGIC ===
# =========================================================================
if __name__ == "__main__":
    today_date_str = datetime.now(eastern).strftime("%Y-%m-%d")
    yesterday_date_str = (datetime.now(eastern) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n--- Running Daily Automated Pull for {today_date_str} and Enrich for {yesterday_date_str} ---")
    # === CHANGED: Log the season being used so it's visible in Actions logs ===
    print(f"🗓️  Season: {CURRENT_SEASON}")

    # --- Step 1: Pull & Enrich Today's Data ---
    today_games = pull_games_and_odds(today_date_str)
    enrich_results_for_games(today_games)

    today_filename = f"data/daily/MLB_Combined_Odds_Results_{today_date_str}.csv"
    if today_games:
        pd.DataFrame(today_games.values()).to_csv(today_filename, index=False)
        print(f"\n✅ Saved today's file to: {today_filename}")
    else:
        print(f"\n⚠️ No games found for today ({today_date_str}). Skipping save.")

    # --- Step 2: Enrich Yesterday's File ---
    yesterday_filename = f"data/daily/MLB_Combined_Odds_Results_{yesterday_date_str}.csv"
    if os.path.exists(yesterday_filename):
        print(f"\n♻️ Enriching yesterday's file: {yesterday_filename}")
        try:
            y_df = pd.read_csv(yesterday_filename, low_memory=False)
            yesterday_games_list = y_df.to_dict(orient="records")
            game_map_for_enrichment = {g["game_id"]: g for g in yesterday_games_list if "game_id" in g}
            enrich_results_for_games(game_map_for_enrichment)
            final_df = pd.DataFrame(game_map_for_enrichment.values())
            final_df.to_csv(yesterday_filename, index=False)
            print(f"✅ Updated yesterday's file: {yesterday_filename}")
        except pd.errors.EmptyDataError:
            print(f"⚠️ Yesterday's file is empty. Skipping enrichment.")
        except Exception as e:
            print(f"❌ Error enriching yesterday's file: {e}")
    else:
        print(f"\n⚠️ No file found for yesterday ({yesterday_filename}) — skipping enrichment.")

    print("\n--- Daily Pull and Enrichment Script Complete ---")
