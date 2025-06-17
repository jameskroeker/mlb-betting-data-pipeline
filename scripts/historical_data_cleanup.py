import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- Configuration ---
MASTER_DIR = "data/master/"
MASTER_FILE_NAME = "master_template.parquet"
MASTER_FILE_PATH = os.path.join(MASTER_DIR, MASTER_FILE_NAME)

# --- Helper Functions (if any, as per previous discussions) ---
# (No additional helper functions were explicitly defined for this script's core logic)

# --- Main Cleanup Function ---
def historical_data_cleanup():
    print(f"--- Running Historical Data Cleanup ---")

    # 1. Load the master file
    if not os.path.exists(MASTER_FILE_PATH):
        print(f"❌ Error: Master file not found at {MASTER_FILE_PATH}. Please ensure it exists.")
        return

    try:
        df_master = pd.read_parquet(MASTER_FILE_PATH)
        print(f"✅ Master file loaded successfully for cleanup. Rows: {len(df_master)}")
    except Exception as e:
        print(f"❌ Error loading master file: {e}")
        return

    # Check if conversion to wide format is needed
    # The 'home_team' column implies wide format (or an intermediate state)
    # The 'is_home' column implies long format
    if 'is_home' in df_master.columns and 'home_team' not in df_master.columns:
        print(f"Detecting 'is_home' column but no 'home_team'. Proceeding with long to wide conversion.")

        # Ensure 'game_date_et' is in a proper format for consistent merging
        # Convert to datetime, coerce errors to NaT, then get just the date part
        # Using utc=True where possible for consistency, then stripping tz
        if 'game_date_et' in df_master.columns:
            df_master['game_date_for_merge'] = pd.to_datetime(df_master['game_date_et'], errors='coerce', utc=True)
            df_master['game_date_for_merge'] = df_master['game_date_for_merge'].dt.date
        else:
            print("❌ Error: 'game_date_et' column not found, cannot create 'game_date_for_merge'. Aborting conversion.")
            return

        # Separate home and away rows
        # Use .copy() to avoid SettingWithCopyWarning
        df_home_rows = df_master[df_master['is_home'] == True].copy()
        df_away_rows = df_master[df_master['is_home'] == False].copy()

        # Rename columns to avoid conflicts after merge
        # These are columns that exist in both home and away rows but need distinct names post-merge
        columns_to_rename = [
            'team', 'team_abbr', 'opponent', 'opponent_abbr', 'Wins', 'Losses', 'Win_Pct',
            'team_streak', 'Win_Streak', 'Loss_Streak', 'home_score', 'away_score',
            'home_1', 'away_1', 'home_2', 'away_2', 'home_3', 'away_3', 'home_4', 'away_4',
            'home_5', 'away_5', 'home_6', 'away_6', 'home_7', 'away_7', 'home_8', 'away_8',
            'home_9', 'away_9', 'team_odds', 'opponent_odds', 'is_home_odds',
            'Run_Line', 'Spread_Price', 'Opp_Spread_Price', 'Total', 'Over_Price', 'Under_Price',
            'h2h_own', 'h2h_opp', 'team_abbr_odds', 'opponent_abbr_odds',
            'run_diff', 'won_game', 'hit_over', 'is_true_duplicate', 'Games_Played'
        ]

        # Filter out merge_keys and game_id_odds from columns_to_rename if they happen to be there
        # This is a precaution as merge_keys are handled directly by the merge `on` parameter
        columns_to_rename_filtered = [col for col in columns_to_rename if col not in ['game_id', 'game_date_for_merge', 'game_id_odds', 'commence_time']]


        # Apply renaming
        df_home_rows = df_home_rows.rename(columns={col: f'home_{col}' for col in columns_to_rename_filtered})
        df_away_rows = df_away_rows.rename(columns={col: f'away_{col}' for col in columns_to_rename_filtered})

        # Identify common columns for merging (game identifiers).
        # Based on user feedback and inspection, game_id is the primary unique identifier for a game.
        # Including game_date_for_merge as a secondary key for robustness, as it's derived consistently.
        merge_keys = ['game_id', 'game_date_for_merge']

        print(f"Merge keys used for conversion: {merge_keys}")

        # Perform the merge to get wide format
        # Use an inner merge to ensure only games with both home and away entries are kept
        # Using suffix for columns that might still conflict despite explicit renames
        df_master_wide = pd.merge(
            df_home_rows,
            df_away_rows,
            on=merge_keys,
            how='inner',
            suffixes=('_home', '_away') # This adds _home/_away suffix if any column names still conflict
        )

        # Drop the original 'is_home' columns if they somehow survived and are not needed
        if 'is_home_home' in df_master_wide.columns:
            df_master_wide.drop(columns=['is_home_home'], inplace=True)
        if 'is_home_away' in df_master_wide.columns:
            df_master_wide.drop(columns=['is_home_away'], inplace=True)

        # Handle potentially redundant game_date columns after merge if suffixes were applied
        # Prioritize the cleaner one or rename as needed.
        # Assuming original 'game_date_et' (now 'home_game_date_et' and 'away_game_date_et')
        # And original 'game_date' (now 'home_game_date' and 'away_game_date')
        # We can drop the _away version for these.
        for col in ['game_date_et', 'start_time_et', 'game_date', 'game_id_odds', 'commence_time', 'season']:
            if f'{col}_away' in df_master_wide.columns:
                df_master_wide.drop(columns=[f'{col}_away'], inplace=True)
                if f'{col}_home' in df_master_wide.columns:
                    df_master_wide.rename(columns={f'{col}_home': col}, inplace=True)
            elif f'{col}_home' in df_master_wide.columns and col not in df_master_wide.columns:
                 df_master_wide.rename(columns={f'{col}_home': col}, inplace=True)


        # Final deduplication step after conversion and merge
        # This is a critical safeguard for any lingering duplicates from the source or merge anomalies
        # Create a robust deduplication key based on unique game identifiers in the wide format
        dedup_key = ['game_id', 'game_date', 'home_team', 'away_team']
        # Convert game_date to date string for dedup key robustness if it's not already
        if 'game_date' in df_master_wide.columns:
             df_master_wide['game_date_str_dedup'] = pd.to_datetime(df_master_wide['game_date'], errors='coerce').dt.strftime('%Y-%m-%d')
             dedup_key = ['game_id', 'game_date_str_dedup', 'home_team', 'away_team']
        else:
            print("Warning: 'game_date' not found for robust deduplication key.")

        initial_wide_rows = len(df_master_wide)
        df_master_wide.drop_duplicates(subset=dedup_key, keep='first', inplace=True)
        rows_after_dedup = len(df_master_wide)

        if initial_wide_rows > rows_after_dedup:
            print(f"Dropped {initial_wide_rows - rows_after_dedup} duplicates after wide conversion.")

        df_master = df_master_wide # Update df_master to the wide format
        print(f"✅ Converted to wide format. New rows: {len(df_master)}")

    else:
        print("Master file appears to be in wide format already or does not need conversion.")

    # 2. General Data Cleaning and Type Conversion (applies to both long and wide format)
    # This section can be expanded based on specific needs

    # Ensure 'game_id' is consistent (e.g., integer type)
    if 'game_id' in df_master.columns:
        df_master['game_id'] = pd.to_numeric(df_master['game_id'], errors='coerce').astype('Int64') # Use Int64 for nullable integer

    # Convert date/time columns to datetime objects
    for col in ['game_date_et', 'start_time_et', 'game_date', 'commence_time']:
        if col in df_master.columns:
            df_master[col] = pd.to_datetime(df_master[col], errors='coerce')

    # Convert numerical columns to appropriate types (e.g., float, Int64)
    numerical_cols = [
        'home_Wins', 'home_Losses', 'home_Win_Pct', 'home_score', 'away_score',
        'away_Wins', 'away_Losses', 'away_Win_Pct',
        'home_1', 'away_1', 'home_2', 'away_2', 'home_3', 'away_3', 'home_4', 'away_4',
        'home_5', 'away_5', 'home_6', 'away_6', 'home_7', 'away_7', 'home_8', 'away_8',
        'home_9', 'away_9',
        'Run_Line', 'Spread_Price', 'Opp_Spread_Price', 'Total', 'Over_Price', 'Under_Price',
        'h2h_own', 'h2h_opp', 'run_diff', 'season', 'Games_Played'
    ]
    # Adjust for renamed columns after wide conversion
    if 'home_team' in df_master.columns and 'is_home' not in df_master.columns: # If already wide format
        numerical_cols_wide = []
        for col in numerical_cols:
            if col.startswith('home_') or col.startswith('away_'):
                numerical_cols_wide.append(col)
            elif col in ['Run_Line', 'Spread_Price', 'Opp_Spread_Price', 'Total', 'Over_Price', 'Under_Price',
                         'h2h_own', 'h2h_opp', 'run_diff', 'season', 'Games_Played']:
                 numerical_cols_wide.append(col)
        numerical_cols = numerical_cols_wide # Use the wide-format specific list


    for col in numerical_cols:
        if col in df_master.columns:
            # Attempt to convert to float first, then to Int64 if no decimals and not NaN
            # This handles potential missing values (NaN) gracefully
            df_master[col] = pd.to_numeric(df_master[col], errors='coerce')
            # For columns that should logically be integers (scores, wins, losses, season)
            if col in ['home_score', 'away_score', 'home_Wins', 'home_Losses', 'away_Wins', 'away_Losses', 'season', 'Games_Played']:
                df_master[col] = df_master[col].astype('Int64', errors='ignore') # Use Int64 for nullable integer

    # Ensure boolean columns are actual booleans
    for col in ['is_true_duplicate', 'won_game', 'hit_over']:
        if f'home_{col}' in df_master.columns and f'away_{col}' in df_master.columns:
             # If both exist, we need to decide which one to keep or how to combine
             # For won_game/hit_over, they relate to a team's performance, so home_won_game and away_won_game makes sense
             pass # Don't convert these here if they're split

        elif col in df_master.columns: # If the original unsplit column exists
             df_master[col] = df_master[col].astype(bool, errors='ignore') # Convert to boolean

    # Drop any temporary columns created for merging/deduplication
    if 'game_date_for_merge' in df_master.columns:
        df_master.drop(columns=['game_date_for_merge'], inplace=True)
    if 'game_date_str_dedup' in df_master.columns:
        df_master.drop(columns=['game_date_str_dedup'], inplace=True)


    # 3. Save the cleaned and converted master file
    try:
        # Save without index to avoid creating an extra 'index' column in the parquet file
        df_master.to_parquet(MASTER_FILE_PATH, index=False)
        print(f"✅ Cleaned and converted master file saved successfully to {MASTER_FILE_PATH}. Final rows: {len(df_master)}")
    except Exception as e:
        print(f"❌ Error saving master file: {e}")

    print(f"--- Historical Data Cleanup Complete ---")

# --- Execute the cleanup ---
if __name__ == "__main__":
    # Ensure the master directory exists
    os.makedirs(MASTER_DIR, exist_ok=True)
    historical_data_cleanup()
