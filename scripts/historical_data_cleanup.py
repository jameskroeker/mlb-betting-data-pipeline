# scripts/historical_data_cleanup.py

import pandas as pd
import numpy as np
import os
import pytz

# === Config ===
MASTER_FILE = "data/master/master_template.parquet"
eastern = pytz.timezone("US/Eastern")

print("\n--- Starting Historical Data Cleanup Script ---")

if not os.path.exists(MASTER_FILE):
    print(f"❌ Error: Master file not found at {MASTER_FILE}. Cannot perform cleanup.")
    exit()

try:
    df_master = pd.read_parquet(MASTER_FILE)
    print(f"✅ Master file loaded successfully for cleanup. Rows: {len(df_master)}")
    print("Original master file columns:", df_master.columns.tolist())

    # --- Step 1: Standardize Critical Column Names and Types ---
    print("\n--- Standardizing column names and types ---")

    # Rename existing columns to match expected schema if they differ
    if 'game_date_et' in df_master.columns and 'game_date' not in df_master.columns:
        df_master.rename(columns={'game_date_et': 'game_date'}, inplace=True)
        print("Renamed 'game_date_et' to 'game_date'.")

    # Ensure date/time columns are proper datetime objects in Eastern Time
    for col in ['game_date', 'start_time_et']:
        if col in df_master.columns:
            # Convert to UTC first if not already, then localize to Eastern
            df_master[col] = pd.to_datetime(df_master[col], errors='coerce', utc=True)
            df_master[col] = df_master[col].dt.tz_convert(eastern)
            print(f"Ensured '{col}' is timezone-aware datetime in Eastern Time.")
        else:
            print(f"Warning: '{col}' not found for standardization.")

    # Convert scores to numeric, handling potential mixed types
    for col in ['home_score', 'away_score']:
        if col in df_master.columns:
            df_master[col] = pd.to_numeric(df_master[col], errors='coerce')
            print(f"Converted '{col}' to numeric.")
        else:
            print(f"Warning: '{col}' not found for numeric conversion.")

    # Ensure game_id is numeric (Int64 for nullable integer)
    if 'game_id' in df_master.columns:
        df_master['game_id'] = pd.to_numeric(df_master['game_id'], errors='coerce').astype('Int64')
        print("Converted 'game_id' to nullable integer.")
    else:
        print("Warning: 'game_id' not found for numeric conversion.")


    # --- Step 2: Infer 'status' and 'winner' columns (more robust creation) ---
    print("\n--- Inferring 'status' and 'winner' columns ---")

    # Handle 'status' column: Create if missing, then infer/fill
    if 'status' not in df_master.columns:
        print("Adding new 'status' column (defaulting to 'Scheduled').")
        df_master['status'] = 'Scheduled' # Initialize as 'Scheduled'
    else:
        print("Existing 'status' column found.")

    # Now fill/update 'status' based on scores for games that have them
    df_master['status'] = np.where(
        df_master['home_score'].notna() & df_master['away_score'].notna(),
        'Finished',
        df_master['status'] # Keep existing status if no scores, or 'Scheduled' if newly created
    )
    df_master['status'] = df_master['status'].astype(str) # Ensure string type
    print("Updated 'status' values based on scores.")

    # Handle 'winner' column: Create if missing, then infer/fill
    if 'winner' not in df_master.columns:
        print("Adding new 'winner' column (defaulting to None).")
        df_master['winner'] = None # Initialize as None
    else:
        print("Existing 'winner' column found.")

    # Ensure scores are numeric for comparison (already done above, but safe to re-ensure)
    df_master['home_score'] = pd.to_numeric(df_master['home_score'], errors='coerce')
    df_master['away_score'] = pd.to_numeric(df_master['away_score'], errors='coerce')

    # Only infer winner for games that are marked 'Finished' and have scores, AND winner is currently missing
    finished_and_winner_missing_mask = (df_master['status'] == 'Finished') & \
                                       df_master['home_score'].notna() & \
                                       df_master['away_score'].notna() & \
                                       df_master['winner'].isnull() # Only fill if winner is not already set

    df_master.loc[finished_and_winner_missing_mask & (df_master['home_score'] > df_master['away_score']), 'winner'] = df_master['home_team']
    df_master.loc[finished_and_winner_missing_mask & (df_master['away_score'] > df_master['home_score']), 'winner'] = df_master['away_team']
    df_master['winner'] = df_master['winner'].astype(str).replace('nan', None) # Ensure string type, convert np.nan to None
    print("Inferred 'winner' values based on scores for finished games where missing.")


    # --- Step 3: Remove Redundant/Old Manually Created Columns ---
    print("\n--- Removing old, manually created W/L/Streak columns ---")
    old_feature_cols = [
        'Wins', 'Losses', 'Win_Pct', 'team_streak', 'Win_Streak', 'Loss_Streak',
        'run_diff', 'won_game', 'hit_over', # These are redundant with what feature_engineering will create
        # These columns indicate a 'long' (team-centric) format that conflicts with our 'wide' (game-centric) format:
        'team', 'team_abbr', 'opponent', 'opponent_abbr', 'is_home',
        'team_odds', 'opponent_odds', 'is_home_odds', 'Run_Line', 'Spread_Price',
        'Opp_Spread_Price', 'Total', 'Over_Price', 'Under_Price', 'h2h_own',
        'h2h_opp', 'team_abbr_odds', 'opponent_abbr_odds', 'Games_Played' # Removed some odds fields that conflict with wide format as well
    ]
    cols_to_drop = [col for col in old_feature_cols if col in df_master.columns]

    if cols_to_drop:
        df_master.drop(columns=cols_to_drop, inplace=True)
        print(f"Dropped redundant columns: {cols_to_drop}")
    else:
        print("No old feature columns found to drop (or they were already removed).")

    # Ensure 'season' is derived from 'game_date'
    if 'game_date' in df_master.columns and 'season' not in df_master.columns:
        df_master['season'] = df_master['game_date'].dt.year
        print("Derived 'season' from 'game_date'.")
    elif 'game_date' in df_master.columns and 'season' in df_master.columns and df_master['season'].isnull().any():
         df_master['season'] = df_master['game_date'].dt.year.fillna(df_master['season'])
         print("Filled missing 'season' values from 'game_date'.")

    # --- Step 4: Final Deduplication and Sorting ---
    print("\n--- Final Deduplication and Sorting ---")

    # Create a robust deduplication key for game-level data
    # Ensure all components of the key are handled properly
    df_master['game_id_str'] = df_master['game_id'].astype(str)
    df_master['game_date_str'] = df_master['game_date'].dt.strftime('%Y-%m-%d').astype(str)
    df_master['home_team_str'] = df_master['home_team'].astype(str)
    df_master['away_team_str'] = df_master['away_team'].astype(str)

    df_master['dedup_key'] = df_master['game_id_str'] + '_' + \
                             df_master['game_date_str'] + '_' + \
                             df_master['home_team_str'] + '_' + \
                             df_master['away_team_str']

    rows_before_dedup = len(df_master)
    df_master.drop_duplicates(subset=['dedup_key'], keep='first', inplace=True)
    rows_after_dedup = len(df_master)
    df_master.drop(columns=['dedup_key', 'game_id_str', 'game_date_str', 'home_team_str', 'away_team_str'], inplace=True) # Remove temporary keys

    print(f"Removed {rows_before_dedup - rows_after_dedup} duplicate rows after cleanup.")

    df_master = df_master.sort_values(by=['season', 'game_date', 'game_id', 'home_team']).reset_index(drop=True)
    print("Master file sorted after cleanup.")

    # --- Save the Cleaned Master File ---
    print(f"\nSaving cleaned master file to: {MASTER_FILE}")
    df_master.to_parquet(MASTER_FILE, index=False)
    print(f"✅ Cleaned master file saved. New total rows: {len(df_master)}")
    print("Updated master file columns:", df_master.columns.tolist())
    print("\n--- Historical Data Cleanup Script Complete ---")

except Exception as e:
    print(f"❌ An error occurred during historical data cleanup: {e}")
