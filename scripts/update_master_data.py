# scripts/update_master_data.py

import os
import pandas as pd
import numpy as np
import glob
from datetime import datetime, timedelta
import pytz

# === CHANGED: Dynamically set current season based on year ===
CURRENT_SEASON = datetime.now().year

def load_team_mapping():
    try:
        team_df = pd.read_excel('data/lookups/MLB_Teams_Template_2025.xlsx')
        return dict(zip(team_df['City and Team'], team_df['Abbreviation']))
    except:
        return {
            'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL',
            'Boston Red Sox': 'BOS', 'Chicago White Sox': 'CWS', 'Chicago Cubs': 'CHC',
            'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE', 'Colorado Rockies': 'COL',
            'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KCR',
            'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA',
            'Milwaukee Brewers': 'MIL', 'Minnesota Twins': 'MIN', 'New York Yankees': 'NYY',
            'New York Mets': 'NYM', 'Athletics': 'ATH', 'Philadelphia Phillies': 'PHI',
            'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SDP', 'San Francisco Giants': 'SFG',
            'Seattle Mariners': 'SEA', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TBR',
            'Texas Rangers': 'TEX', 'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSH'
        }

def get_team_stats_for_season(master_df, season):
    """
    CHANGED: Build team stats for the current season only.
    If no games exist yet for this season (e.g. game 1 of the year),
    every team starts at 0-0 with clean streaks.
    """
    season_df = master_df[master_df['season'] == season].copy()

    # === CHANGED: Get all known team abbreviations from the full master ===
    # so we can initialize any team that hasn't played yet this season
    all_teams = sorted(master_df['team_abbr'].dropna().unique())

    team_stats = {}

    # Initialize every team at 0-0 first
    for team in all_teams:
        team_stats[team] = {
            'wins': 0,
            'losses': 0,
            'win_pct': 0.0,
            'streak': 0,
            'win_streak': 0,
            'loss_streak': 0
        }

    # === CHANGED: Then overwrite with actual season stats if games exist ===
    if len(season_df) > 0:
        for team in season_df['team_abbr'].dropna().unique():
            team_data = season_df[season_df['team_abbr'] == team].sort_values('game_date_et')
            if len(team_data) > 0:
                latest = team_data.iloc[-1]
                streak = 0
                try:
                    streak = int(latest.get('team_streak', 0))
                except:
                    streak = 0
                team_stats[team] = {
                    'wins': int(latest['Wins']),
                    'losses': int(latest['Losses']),
                    'win_pct': float(latest['Win_Pct']),
                    'streak': streak,
                    'win_streak': int(latest.get('Win_Streak', 0)),
                    'loss_streak': int(latest.get('Loss_Streak', 0))
                }

    return team_stats

def map_team_name(team_name, team_mapping):
    if team_name in team_mapping:
        return team_mapping[team_name]
    for full_name, abbr in team_mapping.items():
        if team_name.lower() in full_name.lower():
            return abbr
    print(f"⚠️ Could not map team name: {team_name}")
    return None

def update_team_stats_numeric(team_stats, team_abbr, won):
    stats = team_stats[team_abbr]
    if won:
        stats['wins'] += 1
        stats['loss_streak'] = 0
        stats['win_streak'] += 1
        stats['streak'] = stats['win_streak']
    else:
        stats['losses'] += 1
        stats['win_streak'] = 0
        stats['loss_streak'] += 1
        stats['streak'] = -stats['loss_streak']
    total_games = stats['wins'] + stats['losses']
    stats['win_pct'] = stats['wins'] / total_games if total_games > 0 else 0.0

def create_master_row(game, team_abbr, opponent_abbr, is_home, team_stats, template_row, date):
    row = template_row.copy()

    row['game_id'] = game.get('game_id', '')
    row['game_date_et'] = pd.to_datetime(date)
    row['start_time_et'] = game.get('start_time_et', '')
    row['team_abbr'] = team_abbr
    row['opponent_abbr'] = opponent_abbr
    row['is_home'] = is_home

    if is_home:
        row['team'] = game.get('home_team', '')
        row['opponent'] = game.get('away_team', '')
    else:
        row['team'] = game.get('away_team', '')
        row['opponent'] = game.get('home_team', '')

    # Team statistics (POST-GAME — these reflect record AFTER this game)
    row['Wins'] = team_stats['wins']
    row['Losses'] = team_stats['losses']
    row['Win_Pct'] = round(team_stats['win_pct'], 3)
    row['team_streak'] = team_stats['streak']
    row['Win_Streak'] = team_stats['win_streak']
    row['Loss_Streak'] = team_stats['loss_streak']

    # Game scores
    row['home_score'] = game.get('home_score', 0)
    row['away_score'] = game.get('away_score', 0)

    # Inning scores
    for inning in range(1, 10):
        row[f'home_{inning}'] = game.get(f'home_{inning}', 0)
        row[f'away_{inning}'] = game.get(f'away_{inning}', 0)

    # Betting data
    if is_home:
        row['h2h_own_odds'] = game.get('moneyline_home', '')
        row['h2h_opp_odds'] = game.get('moneyline_away', '')
    else:
        row['h2h_own_odds'] = game.get('moneyline_away', '')
        row['h2h_opp_odds'] = game.get('moneyline_home', '')

    row['Total'] = game.get('total_line', '')
    row['Over_Price_odds'] = game.get('over_odds', '')
    row['Under_Price_odds'] = game.get('under_odds', '')

    # === CHANGED: Use CURRENT_SEASON instead of hardcoded 2025 ===
    row['season'] = CURRENT_SEASON
    row['merge_key'] = f"{game.get('game_id', '')}_{team_abbr}"

    return row

def process_daily_update():
    print("🔄 Starting daily master data update...")
    # === CHANGED: Log season clearly in Actions output ===
    print(f"🗓️  Season: {CURRENT_SEASON}")

    eastern = pytz.timezone("US/Eastern")
    yesterday = (datetime.now(eastern) - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 Target date: {yesterday}")

    # Load master parquet
    master_parquet = "data/master/master_template.parquet"
    if not os.path.exists(master_parquet):
        print("❌ Master parquet file not found!")
        return False

    try:
        master_df = pd.read_parquet(master_parquet)
        master_df['game_date_et'] = pd.to_datetime(master_df['game_date_et'])
    except Exception as e:
        print(f"❌ Error loading master file: {e}")
        return False

    latest_date = master_df['game_date_et'].max().date()
    print(f"📅 Latest date in master data: {latest_date}")

    yesterday_date = datetime.strptime(yesterday, "%Y-%m-%d").date()
    if yesterday_date <= latest_date:
        print(f"✅ Yesterday's data ({yesterday}) already processed")
        return True

    # Load team mapping
    team_mapping = load_team_mapping()

    # === CHANGED: Build stats from current season only (resets to 0-0 for new season) ===
    team_stats = get_team_stats_for_season(master_df, CURRENT_SEASON)
    print(f"📊 Loaded stats for {len(team_stats)} teams (season {CURRENT_SEASON})")

    # Log a few teams so we can verify the reset in Actions logs
    sample_teams = ['NYY', 'SFG', 'LAD', 'BOS']
    for t in sample_teams:
        if t in team_stats:
            s = team_stats[t]
            print(f"   {t}: {s['wins']}-{s['losses']} streak={s['streak']}")

    # Load yesterday's daily file
    yesterday_file = f"data/daily/MLB_Combined_Odds_Results_{yesterday}.csv"
    if not os.path.exists(yesterday_file):
        print(f"📁 No file found for {yesterday}")
        return True

    print(f"⚾ Processing file: {yesterday_file}")

    try:
        daily_df = pd.read_csv(yesterday_file)
    except Exception as e:
        print(f"❌ Error reading {yesterday_file}: {e}")
        return False

    finished_games = daily_df[daily_df['status'] == 'Finished'].copy() if 'status' in daily_df.columns else daily_df.copy()

    if len(finished_games) == 0:
        print(f"⏳ No finished games found in {yesterday_file}")
        return True

    print(f"🎮 Found {len(finished_games)} finished games")

    new_rows = []
    games_processed = 0
    template_row = master_df.iloc[0].copy()

    for _, game in finished_games.iterrows():
        home_team = map_team_name(game['home_team'], team_mapping)
        away_team = map_team_name(game['away_team'], team_mapping)

        if not home_team or not away_team:
            continue

        # === CHANGED: Guard against teams not in stats dict (e.g. expansion/rename edge cases) ===
        if home_team not in team_stats or away_team not in team_stats:
            print(f"⚠️ Skipping game — unknown team: {home_team} vs {away_team}")
            continue

        try:
            home_score = float(game.get('home_score', 0))
            away_score = float(game.get('away_score', 0))
        except (ValueError, TypeError):
            continue

        home_won = home_score > away_score

        update_team_stats_numeric(team_stats, home_team, home_won)
        update_team_stats_numeric(team_stats, away_team, not home_won)

        home_row = create_master_row(game, home_team, away_team, True, team_stats[home_team], template_row, yesterday)
        away_row = create_master_row(game, away_team, home_team, False, team_stats[away_team], template_row, yesterday)

        new_rows.extend([home_row, away_row])
        games_processed += 1

    if not new_rows:
        print("✅ No new games to add")
        return True

    new_df = pd.DataFrame(new_rows)
    new_df['game_date_et'] = pd.to_datetime(new_df['game_date_et'])

    updated_master = pd.concat([master_df, new_df], ignore_index=True)
    updated_master = updated_master.sort_values(['season', 'team_abbr', 'game_date_et']).reset_index(drop=True)

    try:
        updated_master.to_parquet(master_parquet, index=False)
        print(f"💾 Saved parquet: {len(updated_master):,} total rows")
        print(f"✅ Added {games_processed} games ({games_processed * 2} rows) from {yesterday}")
        return True
    except Exception as e:
        print(f"❌ Error saving parquet: {e}")
        return False

if __name__ == "__main__":
    success = process_daily_update()
    if not success:
        exit(1)
