# scripts/daily_pull_and_enrich.py

import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

# === Configuration ===
API_KEY = os.environ.get('API_SPORTS_KEY')
BASE_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
HISTORY_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/scores"
BOOKMAKERS = "draftkings,fanduel,betmgm,caesars" # Specify your preferred bookmakers

# Paths (relative to repo root)
DAILY_DATA_DIR = "data/daily"
os.makedirs(DAILY_DATA_DIR, exist_ok=True) # Ensure directory exists

# Timezone for game dates
eastern = pytz.timezone("US/Eastern")

# === Function to get odds for a specific date ===
def get_odds_for_date(date_str):
    print(f"\n--- Fetching odds for {date_str} ---")
    params = {
        "apiKey": API_KEY,
        "regions": "us", # US regions
        "markets": "h2h,totals", # Head-to-head (moneyline) and Totals (over/under)
        "oddsFormat": "american",
        "dateFormat": "iso",
        "upcoming": "false" # Set to false to get historical/finished games
    }
    # Add date filter for specific historical date
    params["date"] = date_str

    try:
        response = requests.get(HISTORY_URL, params=params)
        response.raise_for_status() # Raise an exception for HTTP errors
        data = response.json()
        print(f"API Request successful. Found {len(data)} games for {date_str}.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching odds for {date_str}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"API Response Content: {e.response.text}")
        return None

# === Process raw game data into a DataFrame ===
def process_game_data(games_data):
    records = []
    for game in games_data:
        game_id = game.get('id')
        sport_title = game.get('sport_title')
        home_team = game.get('home_team')
        away_team = game.get('away_team')
        commence_time_utc = game.get('commence_time') # UTC timestamp
        completed = game.get('completed') # Boolean
        status = game.get('scores', [{}])[0].get('name') if game.get('scores') else 'Scheduled' # Simplified status based on scores

        # Determine winner from scores array
        winner = None
        if completed and game.get('scores'):
            scores_list = game['scores']
            # Assuming first score in array is home, second is away, or they are named
            home_score = next((s['score'] for s in scores_list if s['name'] == home_team), None)
            away_score = next((s['score'] for s in scores_list if s['name'] == away_team), None)

            try:
                if home_score is not None and away_score is not None:
                    home_score_val = float(home_score)
                    away_score_val = float(away_score)
                    if home_score_val > away_score_val:
                        winner = home_team
                    elif away_score_val > home_score_val:
                        winner = away_team
            except ValueError:
                print(f"Warning: Could not convert scores to float for game {game_id}")

        # Extract actual scores if available (for finished games)
        home_final_score = None
        away_final_score = None
        if game.get('scores'):
            for score_obj in game['scores']:
                if score_obj['name'] == home_team:
                    home_final_score = score_obj['score']
                elif score_obj['name'] == away_team:
                    away_final_score = score_obj['score']

        # Determine official status string
        game_status_str = "Scheduled"
        if completed:
            if winner:
                game_status_str = "Finished"
            else:
                game_status_str = "Finished (Tie/Undetermined)" # Edge case for sports that can tie, or data issues
        elif datetime.now(pytz.utc) > datetime.fromisoformat(commence_time_utc.replace('Z', '+00:00')).astimezone(pytz.utc):
            game_status_str = "Live" # If not completed but commence_time is in past

        # Convert UTC commence_time to Eastern Time for consistency
        commence_dt_utc = datetime.fromisoformat(commence_time_utc.replace('Z', '+00:00'))
        commence_dt_eastern = commence_dt_utc.astimezone(eastern)

        # Extract odds for specified bookmakers
        moneyline_home = None
        moneyline_away = None
        total_line = None
        over_odds = None
        under_odds = None

        for site in game.get('bookmakers', []):
            if site['key'] in BOOKMAKERS.split(','):
                for market in site.get('markets', []):
                    if market['key'] == 'h2h':
                        for outcome in market.get('outcomes', []):
                            if outcome['name'] == home_team:
                                moneyline_home = outcome['price']
                            elif outcome['name'] == away_team:
                                moneyline_away = outcome['price']
                    elif market['key'] == 'totals':
                        for outcome in market.get('outcomes', []):
                            total_line = market['outcomes'][0]['point'] # Point is the total line itself
                            if outcome['name'].lower() == 'over':
                                over_odds = outcome['price']
                            elif outcome['name'].lower() == 'under':
                                under_odds = outcome['price']
                # Once we have found odds from one of our preferred bookmakers,
                # we can break or continue to aggregate if needed.
                # For simplicity, we'll take the first one encountered from BOOKMAKERS
                break

        records.append({
            'game_id': game_id,
            'sport_title': sport_title,
            'game_date': commence_dt_eastern.date(), # Just the date part
            'start_time_et': commence_dt_eastern, # Full datetime in Eastern
            'home_team': home_team,
            'away_team': away_team,
            'home_score': home_final_score,
            'away_score': away_final_score,
            'status': game_status_str, # Use the official status string
            'winner': winner, # Explicit winner column
            'moneyline_home': moneyline_home,
            'moneyline_away': moneyline_away,
            'total_line': total_line,
            'over_odds': over_odds,
            'under_odds': under_odds
        })
    return pd.DataFrame(records)

# === Main Execution ===
if __name__ == "__main__":
    # Process data for yesterday
    yesterday = datetime.now(eastern) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    yesterday_data = get_odds_for_date(yesterday_str)

    if yesterday_data:
        df_yesterday = process_game_data(yesterday_data)
        output_path = os.path.join(DAILY_DATA_DIR, f"MLB_Combined_Odds_Results_{yesterday_str}.csv")
        df_yesterday.to_csv(output_path, index=False)
        print(f"✅ Daily combined data saved to {output_path}")
        print(df_yesterday.info()) # Print info to see columns and dtypes
    else:
        print(f"Skipping CSV creation for {yesterday_str} due to API error or no data.")# scripts/daily_pull_and_enrich.py

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

# === Config ===
# Get API_KEY from environment variables (how GitHub Actions passes secrets)
API_KEY = os.environ.get("API_SPORTS_KEY")
if not API_KEY:
    raise ValueError("API_SPORTS_KEY environment variable not set. Please add it as a GitHub Secret.")

HEADERS = {"x-apisports-key": API_KEY}
utc = pytz.utc
eastern = pytz.timezone("US/Eastern")

# Ensure the data/daily directory exists within the repository
# This path is relative to the GitHub Actions runner's working directory (your repo root)
os.makedirs("data/daily", exist_ok=True)

# === Team Name Normalization ===
TEAM_NAME_FIXES = {
    "St.Louis Cardinals": "St. Louis Cardinals"
}

def normalize_team_name(name):
    return TEAM_NAME_FIXES.get(name, name)

# === Utility Functions ===
def safe_inning_scores(scores_dict):
    """Safely retrieves inning scores from a dictionary, returning an empty dict if input is None."""
    return scores_dict.get("innings", {}) if scores_dict else {}

def enrich_results_for_games(games):
    """Enriches game data with scores, winner, and total_result for finished games."""
    print(f"Attempting to enrich {len(games)} games...")
    enriched_count = 0
    for game_id, game in games.items():
        try:
            url = f"https://v1.baseball.api-sports.io/games?id={game_id}"
            response = requests.get(url, headers=HEADERS, timeout=10) # Add timeout
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            data = response.json()

            if not data or not data.get("response"):
                # print(f"⚠️ No API response for game {game_id} or empty response for enrichment.") # Suppressed for brevity in logs unless debugging
                continue

            g = data["response"][0]

            # Only enrich if status is "Finished"
            if g["status"]["long"] != "Finished":
                continue

            scores = g.get("scores", {})
            game["status"] = g["status"]["long"]
            game["home_score"] = scores.get("home", {}).get("total") # Safer access
            game["away_score"] = scores.get("away", {}).get("total") # Safer access

            if game["home_score"] is not None and game["away_score"] is not None:
                if game["home_score"] > game["away_score"]:
                    game["winner"] = game["home_team"]
                elif game["home_score"] < game["away_score"]:
                    game["winner"] = game["away_team"]
                else:
                    game["winner"] = "Draw" # Should be rare in baseball, but good to handle

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
    """Pulls game schedules and odds for a target date."""
    print(f"\n📅 Pulling game schedule and odds for {target_date}")
    api_dates = [target_date, (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")]
    games = {}

    for api_date in api_dates:
        url = f"https://v1.baseball.api-sports.io/games?league=1&season=2025&date={api_date}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=10) # Add timeout
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

                    # Initialize all expected fields, including inning scores
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
                    for i in range(1, 10): # Initialize inning scores
                        game_data[f"home_{i}"] = None
                        game_data[f"away_{i}"] = None
                    games[game_id] = game_data

                except Exception as e:
                    print(f"⚠️ Error processing game metadata for date {api_date} (Game ID: {g.get('id', 'N/A')}): {e}")
        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error fetching games for date {api_date}: {e}")
        except Exception as e:
            print(f"❌ An unexpected error occurred fetching games for date {api_date}: {e}")

    # Pull Odds for all collected games
    for game_id, game in games.items():
        try:
            odds_url = f"https://v1.baseball.api-sports.io/odds?game={game_id}&bookmaker=22" # Using Betway (ID 22)
            response = requests.get(odds_url, headers=HEADERS, timeout=10) # Add timeout
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
                if bet["name"] not in {"Home/Away", "Over/Under"}:
                    continue
                for val in bet.get("values", []):
                    opt = val["value"].lower()
                    odd = val["odd"]
                    if bet["name"] == "Home/Away":
                        if opt == "home":
                            game["moneyline_home"] = odd
                        elif opt == "away":
                            game["moneyline_away"] = odd
                    elif bet["name"] == "Over/Under":
                        if "over" in opt and game["over_odds"] is None:
                            try:
                                game["total_line"] = float(opt.split("over")[1].strip())
                                game["over_odds"] = odd
                            except (ValueError, IndexError):
                                pass
                        elif "under" in opt and game["under_odds"] is None:
                            try:
                                game["under_odds"] = odd
                            except (ValueError, IndexError):
                                pass
        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error fetching odds for game {game_id}: {e}")
        except Exception as e:
            print(f"⚠️ Error fetching or processing odds for game {game_id}: {e}")
            continue

    return games

# =========================================================================
# === MAIN EXECUTION LOGIC FOR DAILY AUTOMATION ===
# =========================================================================
if __name__ == "__main__":
    today_date_str = datetime.now(eastern).strftime("%Y-%m-%d")
    yesterday_date_str = (datetime.now(eastern) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n--- Running Daily Automated Pull for {today_date_str} and Enrich for {yesterday_date_str} ---")

    # --- Step 1: Pull & Enrich Today's Data ---
    today_games = pull_games_and_odds(today_date_str)
    enrich_results_for_games(today_games)

    today_filename = f"data/daily/MLB_Combined_Odds_Results_{today_date_str}.csv"
    if today_games:
        pd.DataFrame(today_games.values()).to_csv(today_filename, index=False)
        print(f"\n✅ Saved today's file to: {today_filename}")
    else:
        print(f"\n⚠️ No games found for today ({today_date_str}). Skipping save to {today_filename}.")

    # --- Step 2: Enrich Yesterday's File (for completed games) ---
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
            print(f"✅ Updated yesterday's file with enriched results: {yesterday_filename}")
        except pd.errors.EmptyDataError:
            print(f"⚠️ Yesterday's file {yesterday_filename} is empty. Skipping enrichment.")
        except Exception as e:
            print(f"❌ Error processing or enriching yesterday's file {yesterday_filename}: {e}")
    else:
        print(f"\n⚠️ No file found for yesterday ({yesterday_filename}) — skipping enrichment.")

    print("\n--- Daily Pull and Enrichment Script Complete ---")
