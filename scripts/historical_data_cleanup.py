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

    # --- NEW Step 0.5: Convert to Wide Format if in Long Format (HISTORICAL DATA SPECIFIC) ---
    if 'is_home' in df_master.columns and 'home_team' not in df_master.columns:
        print("\n--- Detected long format ('is_home' column). Converting to wide (home_team/away_team) format ---")

        # Ensure game_id and date columns are suitable for merging
        df_master['game_date_for_merge'] = pd.to_datetime(df_master['game_date_et'], errors='coerce', utc=True).dt.date


        # Separate home and away team rows for merging
        df_home_rows = df_master[df_master['is_home'] == True].copy()
        df_away_rows = df_master[df_master['is_home'] == False].copy()

        # Rename columns in home rows to be the 'home_' perspective
        df_home_rows.rename(columns={
            'team': 'home_team',
            'team_abbr': 'home_team_abbr',
            'opponent': 'away_team_from_home_perspective', # Temporarily rename to avoid conflict during merge
            'opponent_abbr': 'away_team_abbr_from_home_perspective',
            'home_score': 'home_score_actual', # This is the actual score for the home team
            'away_score': 'away_score_opponent_perspective', # This is the opponent's score from home team's row
        }, inplace=True)

        # Rename columns in away rows to be the 'away_' perspective
        df_away_rows.rename(columns={
            'team': 'away_team',
            'team_abbr': 'away_team_abbr',
            'opponent': 'home_team_from_away_perspective', # Temporarily rename
            'opponent_abbr': 'home_team_abbr_from_away_perspective',
            'home_score': 'away_score_actual', # This is the actual score for the away team
            'away_score': 'home_score_opponent_perspective', # This is the opponent's score from away team's row
        }, inplace=True)

        # Identify common columns for merging (game identifiers).
        merge_keys = ['game_id', 'game_date_for_merge']
        for col in ['start_time_et', 'merge_key', 'game_id_odds', 'commence_time']:
            if col in df_master.columns and col not in merge_keys:
                merge_keys.append(col)

        print(f"Merge keys used: {merge_keys}")

        df_master_wide = pd.merge(
            df_home_rows,
            df_away_rows,
            on=merge_keys,
            suffixes=('_home_perspective', '_away_perspective'),
            how='inner'
        )

        # --- DEBUGGING: Print columns of df_master_wide right after merge ---
        print("\n--- df_master_wide columns after merge (DEBUG) ---")
        print(df_master_wide.columns.tolist())
        print("--- End df_master_wide columns (DEBUG) ---\n")

        # Reconcile scores
        # Check if column exists before accessing
        if 'home_score_actual' in df_master_wide.columns:
            df_master_wide['home_score'] = pd.to_numeric(df_master_wide['home_score_actual'], errors='coerce')
        else:
            raise KeyError("home_score_actual column not found in df_master_wide after merge. Cannot reconcile scores.")

        if 'away_score_actual_away_perspective' in df_master_wide.columns:
            df_master_wide['away_score'] = pd.to_numeric(df_master_wide['away_score_actual_away_perspective'], errors='coerce')
        else:
            # THIS IS THE ERROR LOCATION. The debugging print above should clarify why this is missing.
            raise KeyError("away_score_actual_away_perspective column not found in df_master_wide after merge. Cannot reconcile scores.")


        # Select the desired columns for the new wide format DataFrame.
        final_wide_columns = [
            'game_id',
            'game_date_et',
            'start_time_et',
            'home_team', 'away_team',
            'home_score', 'away_score',
            'sport_title',
            'season',
            'commence_time'
        ]

        # Add inning scores, ensuring they are from the correct home/away perspective
        for i in range(1, 10):
            if f'home_{i}' in df_master_wide.columns:
                final_wide_columns.append(f'home_{i}')
            if f'away_{i}_away_perspective' in df_master_wide.columns:
                final_wide_columns.append(f'away_{i}_away_perspective')

        # Add all relevant odds and other truly game-level info
        for col in ['Run_Line', 'Spread_Price', 'Opp_Spread_Price', 'Total', 'Over_Price', 'Under_Price',
                    'h2h_own', 'h2h_opp']:
            if col in df_master_wide.columns and col not in final_wide_columns:
                final_wide_columns.append(col)
            elif f"{col}_home_perspective" in df_master_wide.columns and f"{col}_home_perspective" not in final_wide_columns:
                 final_wide_columns.append(f"{col}_home_perspective")


        final_wide_columns = list(dict.fromkeys(final_wide_columns))
        # Ensure we only select columns that actually exist in df_master_wide
        df_master = df_master_wide[[col for col in final_wide_columns if col in df_master_wide.columns]].copy()

        # Rename inning score columns back to just 'home_1', 'away_1' etc.
        for i in range(1, 10):
            if f'away_{i}_away_perspective' in df_master.columns:
                df_master.rename(columns={f'away_{i}_away_perspective': f'away_{i}'}, inplace=True)


        # Clean up game_date and season (post-wide conversion)
        if 'game_date_et' in df_master.columns and 'game_date' not in df_master.columns:
            df_master.rename(columns={'game_date_et': 'game_date'}, inplace=True)
            print("Renamed 'game_date_et' to 'game_date'.")
        elif 'game_date' in df_master.columns:
            df_master['game_date'] = pd.to_datetime(df_master['game_date'], errors='coerce').dt.date

        if 'season' not in df_master.columns and 'game_date' in df_master.columns:
            df_master['season'] = pd.to_datetime(df_master['game_date'], errors='coerce').dt.year
            print("Derived 'season' from 'game_date'.")
        elif 'season' in df_master.columns and 'game_date' in df_master.columns and df_master['season'].isnull().any():
            df_master['season'] = pd.to_datetime(df_master['game_date'], errors='coerce').dt.year.fillna(df_master['season'])
            print("Filled missing 'season' values from 'game_date'.")


        # Drop temporary merge column
        if 'game_date_for_merge' in df_master.columns:
            df_master.drop(columns=['game_date_for_merge'], inplace=True)


        print(f"✅ Converted to wide format. New rows: {len(df_master)}")
        print("Columns after wide format conversion:", df_master.columns.tolist())
    else:
        print("\n--- Data already in wide format (home_team/away_team) or 'is_home' not found. Skipping wide format conversion. ---")


    # --- Step 1: Standardize Critical Column Names and Types ---
    print("\n--- Standardizing column names and types ---")
    for col in ['game_date', 'start_time_et', 'commence_time']:
        if col in df_master.columns:
            df_master[col] = pd.to_datetime(df_master[col], errors='coerce', utc=True)
            df_master[col] = df_master[col].dt.tz_convert(eastern)
            print(f"Ensured '{col}' is timezone-aware datetime in Eastern Time.")
        else:
            print(f"Warning: '{col}' not found for standardization.")

    for col in ['home_score', 'away_score']:
        if col in df_master.columns:
            df_master[col] = pd.to_numeric(df_master[col], errors='coerce')
            print(f"Converted '{col}' to numeric.")
        else:
            print(f"Warning: '{col}' not found for numeric conversion.")

    if 'game_id' in df_master.columns:
        df_master['game_id'] = pd.to_numeric(df_master['game_id'], errors='coerce').astype('Int64')
        print("Converted 'game_id' to nullable integer.")
    else:
        print("Warning: 'game_id' not found for numeric conversion.")


    # --- Step 2: Infer 'status' and 'winner' columns ---
    print("\n--- Inferring 'status' and 'winner' columns ---")

    if 'status' not in df_master.columns:
        print("Adding new 'status' column (defaulting to 'Scheduled').")
        df_master['status'] = 'Scheduled'
    else:
        print("Existing 'status' column found.")

    df_master['status'] = np.where(
        df_master['home_score'].notna() & df_master['away_score'].notna(),
        'Finished',
        df_master['status']
    )
    df_master['status'] = df_master['status'].astype(str)
    print("Updated 'status' values based on scores.")

    if 'winner' not in df_master.columns:
        print("Adding new 'winner' column (defaulting to None).")
        df_master['winner'] = None
    else:
        print("Existing 'winner' column found.")

    df_master['home_score'] = pd.to_numeric(df_master['home_score'], errors='coerce')
    df_master['away_score'] = pd.to_numeric(df_master['away_score'], errors='coerce')

    finished_and_winner_missing_mask = (df_master['status'] == 'Finished') & \
                                       df_master['home_score'].notna() & \
                                       df_master['away_score'].notna() & \
                                       df_master['winner'].isnull()

    if 'home_team' in df_master.columns and 'away_team' in df_master.columns:
        df_master.loc[finished_and_winner_missing_mask & (df_master['home_score'] > df_master['away_score']), 'winner'] = df_master['home_team']
        df_master.loc[finished_and_winner_missing_mask & (df_master['away_score'] > df_master['home_score']), 'winner'] = df_master['away_team']
        df_master['winner'] = df_master['winner'].astype(str).replace('nan', None)
        print("Inferred 'winner' values based on scores for finished games where missing.")
    else:
        print("Warning: 'home_team' or 'away_team' columns not found after wide conversion. Cannot infer winner.")


    # --- Step 3: Remove Redundant/Old Manually Created Columns ---
    print("\n--- Removing old, manually created W/L/Streak columns ---")
    old_feature_cols = [
        'Wins', 'Losses', 'Win_Pct', 'team_streak', 'Win_Streak', 'Loss_Streak',
        'run_diff', 'won_game', 'hit_over',
        'team', 'team_abbr', 'opponent', 'opponent_abbr', 'is_home',
        'game_id_odds', 'team_odds', 'opponent_odds', 'is_home_odds',
        'team_abbr_odds', 'opponent_abbr_odds', 'Games_Played',
        'merge_key',
        'away_team_from_home_perspective', 'away_team_abbr_from_home_perspective',
        'home_score_actual', 'away_score_opponent_perspective',
        'home_team_from_away_perspective', 'home_team_abbr_from_away_perspective',
        'away_score_actual', 'home_score_opponent_perspective',
        # Any other suffixed columns from the merge that weren't explicitly handled or needed
        *[col for col in df_master.columns if col.endswith('_home_perspective') or col.endswith('_away_perspective')]
    ]
    cols_to_drop = [col for col in old_feature_cols if col in df_master.columns]

    if cols_to_drop:
        df_master.drop(columns=cols_to_drop, inplace=True)
        print(f"Dropped redundant columns: {cols_to_drop}")
    else:
        print("No old feature columns found to drop (or they were already removed).")


    if 'game_date' in df_master.columns and 'season' not in df_master.columns:
        df_master['season'] = pd.to_datetime(df_master['game_date'], errors='coerce').dt.year
        print("Derived 'season' from 'game_date'.")
    elif 'game_date' in df_master.columns and 'season' in df_master.columns and df_master['season'].isnull().any():
         df_master['season'] = pd.to_datetime(df_master['game_date'], errors='coerce').dt.year.fillna(df_master['season'])
         print("Filled missing 'season' values from 'game_date'.")

    # --- Step 4: Final Deduplication and Sorting ---
    print("\n--- Final Deduplication and Sorting ---")

    if 'game_id' in df_master.columns and df_master['game_id'].notna().any():
        df_master['dedup_key'] = df_master['game_id'].astype(str) + '_' + \
                                 pd.to_datetime(df_master['game_date'], errors='coerce').dt.strftime('%Y-%m-%d').astype(str) + '_' + \
                                 df_master['home_team'].astype(str) + '_' + \
                                 df_master['away_team'].astype(str)
    else:
         df_master['dedup_key'] = pd.to_datetime(df_master['game_date'], errors='coerce').dt.strftime('%Y-%m-%d').astype(str) + '_' + \
                                 df_master['home_team'].astype(str) + '_' + \
                                 df_master['away_team'].astype(str)

    rows_before_dedup = len(df_master)
    df_master.drop_duplicates(subset=['dedup_key'], keep='first', inplace=True)
    rows_after_dedup = len(df_master)
    df_master.drop(columns=['dedup_key'], inplace=True)

    print(f"Removed {rows_before_dedup - rows_after_dedup} duplicate rows after cleanup.")

    sort_cols = ['season', 'game_date']
    if 'game_id' in df_master.columns:
        sort_cols.append('game_id')
    sort_cols.extend(['home_team', 'away_team'])

    df_master = df_master.sort_values(by=sort_cols).reset_index(drop=True)
    print("Master file sorted after cleanup.")

    # --- Save the Cleaned Master File ---
    print(f"\nSaving cleaned master file to: {MASTER_FILE}")
    df_master.to_parquet(MASTER_FILE, index=False)
    print(f"✅ Cleaned master file saved. New total rows: {len(df_master)}")
    print("Updated master file columns:", df_master.columns.tolist())
    print("\n--- Historical Data Cleanup Script Complete ---")

except Exception as e:
    print(f"❌ An error occurred during historical data cleanup: {e}")
