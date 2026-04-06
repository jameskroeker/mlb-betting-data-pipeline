# scripts/daily_pull_and_enrich.py

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

# === Config ===
API_KEY = os.environ.get("API_SPORTS_KEY")
if not API_KEY:
    raise ValueError("API_SPORTS_KEY environment variable not set.")

HEADERS = {"x-apisports-key": API_KEY}

# === CHANGED: Dynamic season year ===
CURRENT_SEASON = datetime.now().year

# === Consensus odds target — decimal equivalent of -110 ===
TARGET_ODDS = 1.909

utc = pytz.utc
eastern = pytz.timezone("US/Eastern")

os.makedirs("data/daily", exist_ok=True)

# === Team Name Normalization ===
TEAM_NAME_FIXES = {
    "St.Louis Cardinals": "St. Louis Cardinals"
}

def normalize_team_name(name):
    return TEAM_NAME_FIXES.get(name, name)

def safe_inning_scores(scores_dict):
    return scores_dict.get("innings", {}) if scores_dict else {}

def fetch_odds_from_bookmaker(game_id, bookmaker_id):
    """Fetch raw bets list from a specific bookmaker. Returns bets list or None."""
    try:
        odds_url = f"https://v1.baseball.api-sports.io/odds?game={game_id}&bookmaker={bookmaker_id}"
        response = requests.get(odds_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        odds_data = response.json()

        if not odds_data or not odds_data.get("response"):
            return None
        bookmakers_data = odds_data["response"][0].get("bookmakers")
        if not bookmakers_data:
            return None
        bets = bookmakers_data[0].get("bets")
        return bets if bets else None
    except Exception as e:
        print(f"⚠️ Error fetching odds from bookmaker {bookmaker_id} for game {game_id}: {e}")
        return None


def pull_odds_for_game(game_id, game):
    """Pull and parse odds for a single game. Tries Pinnacle (4) then Marathon (10) as fallback."""
    # Try Pinnacle first, then Marathon as fallback
    bets = None
    bookmaker_used = None
    for bk_id, bk_name in [(4, 'Pinnacle'), (10, 'Marathon')]:
        bets = fetch_odds_from_bookmaker(game_id, bk_id)
        if bets:
            bookmaker_used = bk_name
            if bk_id != 4:
                print(f"  ⚠️ Pinnacle unavailable — using {bk_name} for game {game_id}")
            break

    if not bets:
        print(f"  ❌ No odds available from any bookmaker for game {game_id}")
        return False

    try:

        for bet in bets:
            if bet["name"] == "Home/Away":
                for val in bet.get("values", []):
                    opt = val["value"].lower()
                    if opt == "home":
                        game["moneyline_home"] = val["odd"]
                    elif opt == "away":
                        game["moneyline_away"] = val["odd"]

            elif bet["name"] == "Over/Under":
                totals_by_line = {}
                for val in bet.get("values", []):
                    try:
                        parts = val["value"].split(" ")
                        side = parts[0].lower()
                        line = float(parts[1])
                    except (IndexError, ValueError):
                        continue
                    if line not in totals_by_line:
                        totals_by_line[line] = {}
                    totals_by_line[line][side] = float(val["odd"])

                best_line = None
                best_distance = float("inf")
                for line, sides in totals_by_line.items():
                    if "over" in sides and "under" in sides:
                        avg_dist = (abs(sides["over"] - TARGET_ODDS) +
                                   abs(sides["under"] - TARGET_ODDS)) / 2
                        if avg_dist < best_distance:
                            best_distance = avg_dist
                            best_line = line

                if best_line is not None:
                    game["total_line"] = best_line
                    game["over_odds"] = totals_by_line[best_line].get("over")
                    game["under_odds"] = totals_by_line[best_line].get("under")

        return True

    except Exception as e:
        print(f"⚠️ Error parsing odds for game {game_id}: {e}")
        return False
        return False

def re_enrich_missing_odds(games):
    """
    CHANGED: Re-pull odds for games where odds are still null.
    Fixes timing issue where Pinnacle posts lines after the initial pull.
    """
    missing = {
        gid: game for gid, game in games.items()
        if game.get("moneyline_home") is None or game.get("total_line") is None
    }

    if not missing:
        print("✅ No missing odds — all games have complete data")
        return 0

    print(f"🔄 Re-enriching odds for {len(missing)} games with missing data...")
    fixed = 0
    for game_id, game in missing.items():
        if pull_odds_for_game(game_id, game):
            print(f"  ✅ Game {game_id} ({game.get('home_team')} vs {game.get('away_team')}): "
                  f"ML={game.get('moneyline_home')} Total={game.get('total_line')}")
            fixed += 1
        else:
            print(f"  ⚠️ Game {game_id} ({game.get('home_team')} vs {game.get('away_team')}): "
                  f"still no odds available")

    print(f"🔄 Re-enrichment complete: {fixed}/{len(missing)} games fixed")
    return fixed

def enrich_results_for_games(games):
    """Enrich game data with scores and innings for finished games."""
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
                    # CHANGED: Three-way total result — Push when exact line hit
                    if total > game["total_line"]:
                        game["total_result"] = "Over"
                    elif total < game["total_line"]:
                        game["total_result"] = "Under"
                    else:
                        game["total_result"] = "Push"
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
    """Pull game schedules and odds for a target date."""
    print(f"\n📅 Pulling game schedule and odds for {target_date}")
    api_dates = [target_date, (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")]
    games = {}

    for api_date in api_dates:
        # CHANGED: Dynamic season year
        url = f"https://v1.baseball.api-sports.io/games?league=1&season={CURRENT_SEASON}&date={api_date}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data or not data.get("response"):
                print(f"⚠️ No API response for date {api_date}.")
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
                    print(f"⚠️ Error processing game (Game ID: {g.get('id', 'N/A')}): {e}")
        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error fetching games for {api_date}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error for {api_date}: {e}")

    # Pull odds for all games using shared function
    odds_success = 0
    for game_id, game in games.items():
        if pull_odds_for_game(game_id, game):
            odds_success += 1

    print(f"📊 Odds pulled for {odds_success}/{len(games)} games")
    return games

# =========================================================================
# === MAIN EXECUTION LOGIC ===
# =========================================================================
if __name__ == "__main__":
    today_date_str = datetime.now(eastern).strftime("%Y-%m-%d")
    yesterday_date_str = (datetime.now(eastern) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n--- Running Daily Automated Pull for {today_date_str} ---")
    print(f"🗓️  Season: {CURRENT_SEASON}")

    # --- Step 1: Pull today's games and odds ---
    today_games = pull_games_and_odds(today_date_str)
    enrich_results_for_games(today_games)

    today_filename = f"data/daily/MLB_Combined_Odds_Results_{today_date_str}.csv"
    if today_games:
        pd.DataFrame(today_games.values()).to_csv(today_filename, index=False)
        print(f"\n✅ Saved today's file: {today_filename}")
    else:
        print(f"\n⚠️ No games found for today ({today_date_str}). Skipping save.")

    # --- Step 2: Enrich yesterday's file with scores and missing odds ---
    yesterday_filename = f"data/daily/MLB_Combined_Odds_Results_{yesterday_date_str}.csv"
    if os.path.exists(yesterday_filename):
        print(f"\n♻️ Enriching yesterday's file: {yesterday_filename}")
        try:
            y_df = pd.read_csv(yesterday_filename, low_memory=False)
            yesterday_games_list = y_df.to_dict(orient="records")
            game_map = {g["game_id"]: g for g in yesterday_games_list if "game_id" in g}

            # CHANGED: Re-enrich missing odds from yesterday first
            re_enrich_missing_odds(game_map)

            # Then enrich scores for finished games
            enrich_results_for_games(game_map)

            final_df = pd.DataFrame(game_map.values())
            final_df.to_csv(yesterday_filename, index=False)
            print(f"✅ Updated yesterday's file: {yesterday_filename}")
        except pd.errors.EmptyDataError:
            print(f"⚠️ Yesterday's file is empty. Skipping.")
        except Exception as e:
            print(f"❌ Error enriching yesterday's file: {e}")
    else:
        print(f"\n⚠️ No file found for yesterday ({yesterday_filename}) — skipping.")

    # --- Step 3: CHANGED: Re-enrich today's odds if any were missing at pull time ---
    if today_games and os.path.exists(today_filename):
        print(f"\n🔄 Checking today's file for missing odds...")
        fixed = re_enrich_missing_odds(today_games)
        if fixed > 0:
            pd.DataFrame(today_games.values()).to_csv(today_filename, index=False)
            print(f"✅ Saved today's file with re-enriched odds: {today_filename}")

    print("\n--- Daily Pull and Enrichment Script Complete ---")
