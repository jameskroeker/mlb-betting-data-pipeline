# scripts/feature_engineering.py

import pandas as pd
import os
import numpy as np

# === Config ===
MASTER_FILE = "data/master/master_template.parquet"
# This script will save the enhanced master file back to the same path.
# If you prefer to save it as a new file (e.g., 'enhanced_master.parquet'),
# change this path.

print("\n--- Starting Feature Engineering Script ---")

if not os.path.exists(MASTER_FILE):
    print(f"❌ Error: Master file not found at {MASTER_FILE}. Cannot perform feature engineering.")
    exit() # Exit if master file doesn't exist

try:
    df = pd.read_parquet(MASTER_FILE)
    print(f"✅ Master file loaded successfully. Rows: {len(df)}")

    # Ensure correct data types and sorting for calculations
    df['game_date'] = pd.to_datetime(df['game_date'])
    df['start_time_et'] = pd.to_datetime(df['start_time_et'])
    df['game_id'] = pd.to_numeric(df['game_id'], errors='coerce').astype('Int64') # Use nullable integer
    df = df.sort_values(by=['game_date', 'start_time_et', 'game_id']).reset_index(drop=True)
    print("Data sorted by date and game ID.")

    # --- 1. Calculate Run Differential ---
    print("Calculating Run Differential...")
    # Run differential from the home team's perspective
    df['home_run_differential'] = df['home_score'] - df['away_score']
    # Run differential from the away team's perspective
    df['away_run_differential'] = df['away_score'] - df['home_score']
    print("Run Differential calculated.")

    # --- 2. Calculate Betting Results ---
    print("Calculating Betting Results (Moneyline and Over/Under)...")

    # Moneyline Results
    # Initialize as NaN or None for games not applicable/finished
    df['moneyline_bet_result'] = None # 'Win', 'Loss' for the home team's moneyline bet
    df['home_moneyline_bet_result'] = None
    df['away_moneyline_bet_result'] = None

    # Only process if scores and moneyline odds are available and game is finished
    finished_games_with_ml = df[
        (df['status'] == 'Finished') &
        df['moneyline_home'].notna() &
        df['moneyline_away'].notna() &
        df['home_score'].notna() &
        df['away_score'].notna()
    ].copy()

    if not finished_games_with_ml.empty:
        # Home Team Moneyline Result
        finished_games_with_ml.loc[finished_games_with_ml['winner'] == finished_games_with_ml['home_team'], 'home_moneyline_bet_result'] = 'Win'
        finished_games_with_ml.loc[finished_games_with_ml['winner'] == finished_games_with_ml['away_team'], 'home_moneyline_bet_result'] = 'Loss'

        # Away Team Moneyline Result
        finished_games_with_ml.loc[finished_games_with_ml['winner'] == finished_games_with_ml['away_team'], 'away_moneyline_bet_result'] = 'Win'
        finished_games_with_ml.loc[finished_games_with_ml['winner'] == finished_games_with_ml['home_team'], 'away_moneyline_bet_result'] = 'Loss'

        # Merge results back to main DataFrame
        df.update(finished_games_with_ml[['game_id', 'home_moneyline_bet_result', 'away_moneyline_bet_result']].set_index('game_id'))
    else:
        print("No finished games with moneyline data to process.")

    # Total (Over/Under) Results
    df['total_bet_result'] = None # 'Over', 'Under', 'Push'

    finished_games_with_total = df[
        (df['status'] == 'Finished') &
        df['total_line'].notna() &
        df['over_odds'].notna() &
        df['under_odds'].notna() &
        df['home_score'].notna() &
        df['away_score'].notna()
    ].copy()

    if not finished_games_with_total.empty:
        total_runs = finished_games_with_total['home_score'] + finished_games_with_total['away_score']
        finished_games_with_total.loc[total_runs > finished_games_with_total['total_line'], 'total_bet_result'] = 'Over'
        finished_games_with_total.loc[total_runs < finished_games_with_total['total_line'], 'total_bet_result'] = 'Under'
        finished_games_with_total.loc[total_runs == finished_games_with_total['total_line'], 'total_bet_result'] = 'Push'

        df.update(finished_games_with_total[['game_id', 'total_bet_result']].set_index('game_id'))
    else:
        print("No finished games with total line data to process.")
    print("Betting Results calculated.")

    # --- 3. Calculate Win/Loss Records and Win Streaks for Each Team for Each Season ---
    print("Calculating Win/Loss Records and Win Streaks...")

    # Create a temporary DataFrame where each game appears twice (once for home, once for away)
    # This simplifies per-team calculations
    team_games = []

    for _, row in df.iterrows():
        # Home team perspective
        team_games.append({
            'game_id': row['game_id'],
            'game_date': row['game_date'],
            'season': row['season'],
            'team': row['home_team'],
            'opponent': row['away_team'],
            'team_score': row['home_score'],
            'opponent_score': row['away_score'],
            'is_winner': (row['winner'] == row['home_team']) if pd.notna(row['winner']) else None,
            'is_home': True,
            'status': row['status']
        })
        # Away team perspective
        team_games.append({
            'game_id': row['game_id'],
            'game_date': row['game_date'],
            'season': row['season'],
            'team': row['away_team'],
            'opponent': row['home_team'],
            'team_score': row['away_score'],
            'opponent_score': row['home_score'],
            'is_winner': (row['winner'] == row['away_team']) if pd.notna(row['winner']) else None,
            'is_home': False,
            'status': row['status']
        })

    df_team_games = pd.DataFrame(team_games)
    # Filter for games that are finished and have a winner defined
    df_team_games_finished = df_team_games[
        (df_team_games['status'] == 'Finished') &
        df_team_games['is_winner'].notna()
    ].copy()

    # Sort again for cumulative calculations
    df_team_games_finished = df_team_games_finished.sort_values(
        by=['season', 'team', 'game_date', 'game_id']
    ).reset_index(drop=True)

    # Initialize columns
    df_team_games_finished['wins'] = 0
    df_team_games_finished['losses'] = 0
    df_team_games_finished['win_streak'] = 0 # Positive for win streak, negative for loss streak

    # Group by season and team to calculate cumulative records and streaks
    def calculate_records_and_streaks(group):
        wins_count = 0
        losses_count = 0
        current_streak = 0
        results = []

        for _, row in group.iterrows():
            if row['is_winner']: # Team won
                wins_count += 1
                if current_streak >= 0:
                    current_streak += 1
                else: # Was on a loss streak, now won
                    current_streak = 1
            else: # Team lost
                losses_count += 1
                if current_streak <= 0:
                    current_streak -= 1
                else: # Was on a win streak, now lost
                    current_streak = -1

            results.append({
                'game_id': row['game_id'],
                'team': row['team'],
                'wins_season_cumulative': wins_count,
                'losses_season_cumulative': losses_count,
                'win_streak_team_cumulative': current_streak
            })
        return pd.DataFrame(results)

    # Apply the function to each team within each season
    if not df_team_games_finished.empty:
        print("Applying cumulative win/loss/streak calculations...")
        # Use .copy() to avoid SettingWithCopyWarning
        calculated_records = df_team_games_finished.groupby(['season', 'team'], group_keys=False).apply(calculate_records_and_streaks).copy()

        # Merge these cumulative stats back to the main df.
        # This will require merging twice (once for home team, once for away team)
        # to ensure home_team_wins, away_team_wins etc are correct per game row.

        # Merge for home team stats
        df = df.merge(
            calculated_records.rename(columns={
                'wins_season_cumulative': 'home_wins_season',
                'losses_season_cumulative': 'home_losses_season',
                'win_streak_team_cumulative': 'home_win_streak'
            }),
            left_on=['game_id', 'home_team'],
            right_on=['game_id', 'team'],
            how='left'
        ).drop(columns=['team'], errors='ignore') # Drop the temporary 'team' column from the merged part

        # Merge for away team stats
        df = df.merge(
            calculated_records.rename(columns={
                'wins_season_cumulative': 'away_wins_season',
                'losses_season_cumulative': 'away_losses_season',
                'win_streak_team_cumulative': 'away_win_streak'
            }),
            left_on=['game_id', 'away_team'],
            right_on=['game_id', 'team'],
            how='left'
        ).drop(columns=['team'], errors='ignore') # Drop the temporary 'team' column from the merged part
        print("Win/Loss Records and Win Streaks calculated and merged.")
    else:
        print("No finished games to calculate win/loss records and streaks for.")

    # --- Save the Enhanced Master File ---
    print(f"\nSaving enhanced master file to: {MASTER_FILE}")
    df.to_parquet(MASTER_FILE, index=False)
    print(f"✅ Enhanced master file saved. New total rows: {len(df)}")
    print("\n--- Feature Engineering Script Complete ---")

except Exception as e:
    print(f"❌ An error occurred during feature engineering: {e}")
