import pandas as pd
import os
from datetime import datetime, timedelta
import pytz
import numpy as np

# === Config ===
MASTER_FILE = "data/master/master_template.parquet" # Updated to your specified master file name
DAILY_DATA_DIR = "data/daily/"
eastern = pytz.timezone("US/Eastern")

# Ensure the master directory exists
os.makedirs(os.path.dirname(MASTER_FILE), exist_ok=True)
os.makedirs(DAILY_DATA_DIR, exist_ok=True)

eastern = pytz.timezone("US/Eastern")

# Define the master schema
master_schema = {
    'game_id': 'Int64', 'game_date_et': 'datetime64[ns, US/Eastern]', 'start_time_et': 'datetime64[ns, US/Eastern]',
    'home_team': 'object', 'home_team_abbr': 'object', 'home_opponent': 'object', 'home_opponent_abbr': 'object',
    'home_Wins': 'Int64', 'home_Losses': 'Int64', 'home_Win_Pct': 'float64', 'home_team_streak': 'object',
    'home_Win_Streak': 'Int64', 'home_Loss_Streak': 'Int64', 'home_home_score': 'Int64', 'home_away_score': 'Int64',
    'home_home_1': 'Int64', 'home_away_1': 'Int64', 'home_home_2': 'Int64', 'home_away_2': 'Int64',
    'home_home_3': 'Int64', 'home_away_3': 'Int64', 'home_home_4': 'Int64', 'home_away_4': 'Int64',
    'home_home_5': 'Int64', 'home_away_5': 'Int64', 'home_home_6': 'Int64', 'home_away_6': 'Int64',
    'home_home_7': 'Int64', 'home_away_7': 'Int64', 'home_home_8': 'Int64', 'home_away_8': 'Int64',
    'home_home_9': 'Int64', 'home_away_9': 'Int64', 'merge_key_home': 'object',
    'game_date': 'datetime64[ns, US/Eastern]', 'game_id_odds': 'Int64', 'commence_time': 'datetime64[ns, US/Eastern]',
    'home_team_odds': 'float64', 'home_opponent_odds': 'float64', 'home_is_home_odds': 'boolean',
    'home_Run_Line': 'float64', 'home_Spread_Price': 'float64', 'home_Opp_Spread_Price': 'float64',
    'home_Total': 'float64', 'home_Over_Price': 'float64', 'home_Under_Price': 'float64',
    'home_h2h_own': 'float64', 'home_h2h_opp': 'float64', 'home_team_abbr_odds': 'object',
    'home_opponent_abbr_odds': 'object', 'home_run_diff': 'Int64', 'home_won_game': 'boolean',
    'home_hit_over': 'boolean', 'home_is_true_duplicate': 'boolean', 'season': 'Int64', 'home_Games_Played': 'Int64',

    'away_team': 'object', 'away_team_abbr': 'object', 'away_opponent': 'object', 'away_opponent_abbr': 'object',
    'away_Wins': 'Int64', 'away_Losses': 'Int64', 'away_Win_Pct': 'float64', 'away_team_streak': 'object',
    'away_Win_Streak': 'Int64', 'away_Loss_Streak': 'Int64', 'away_home_score': 'Int64', 'away_away_score': 'Int64',
    'away_home_1': 'Int64', 'away_away_1': 'Int64', 'away_home_2': 'Int64', 'away_away_2': 'Int64',
    'away_home_3': 'Int64', 'away_away_3': 'Int64', 'away_home_4': 'Int64', 'away_away_4': 'Int64',
    'away_home_5': 'Int64', 'away_away_5': 'Int64', 'away_home_6': 'Int64', 'away_away_6': 'Int64',
    'away_home_7': 'Int64', 'away_away_7': 'Int64', 'away_home_8': 'Int64', 'away_away_8': 'Int64',
    'away_home_9': 'Int64', 'away_away_9': 'Int64', 'merge_key_away': 'object',
    'away_team_odds': 'float64', 'away_opponent_odds': 'float64', 'away_is_home_odds': 'boolean',
    'away_Run_Line': 'float64', 'away_Spread_Price': 'float64', 'away_Opp_Spread_Price': 'float64',
    'away_Total': 'float64', 'away_Over_Price': 'float64', 'away_Under_Price': 'float64',
    'away_h2h_own': 'float64', 'away_h2h_opp': 'float64', 'away_team_abbr_odds': 'object',
    'away_opponent_abbr_odds': 'object', 'away_run_diff': 'Int64', 'away_won_game': 'boolean',
    'away_hit_over': 'boolean', 'away_is_true_duplicate': 'boolean', 'away_Games_Played': 'Int64',
    'run_diff': 'Int64', 'game_date_for_merge': 'datetime64[ns, US/Eastern]',
}


def process_daily_file(file_path: str):
    """
    Processes a single daily MLB betting data file, filters for finished games,
    transforms and cleans the data, and appends it to the master Parquet file.
    """
    print(f"\n--- Attempting to process completed game data from {file_path} ---")

    if not os.path.exists(file_path):
        print(f"❌ Error: Daily CSV not found at {file_path}. Skipping append to master.")
        return

    try:
        df_daily = pd.read_csv(file_path, low_memory=False)
        df_daily.columns = df_daily.columns.str.strip()  # Strip whitespace from column names
        print(f"✅ Daily CSV loaded successfully. Rows: {len(df_daily)}")

        # --- Filter for FINISHED games only before processing ---
        df_finished_games = df_daily[df_daily['status'] == 'Finished'].copy()
        print(f"Found {len(df_finished_games)} finished games to process.")

        if df_finished_games.empty:
            print("No finished games found in daily data. Skipping append to master.")
            return

        # --- Rename columns to match master template schema ---
        daily_csv_column_mapping = {
            'game_id': 'game_id',
            'game_date': 'game_date_et_temp', # Temporary name for the date part
            'start_time_et': 'start_time_et_str', # Temporary name for the time string
            'home_team': 'home_team',
            'away_team': 'home_opponent', # Map away_team from daily to home_opponent in master
            'moneyline_home': 'home_team_odds',
            'moneyline_away': 'home_opponent_odds',
            'total_line': 'home_Total',
            'over_odds': 'home_Over_Price',
            'under_odds': 'home_Under_Price',
            'home_score': 'home_home_score',
            'away_score': 'home_away_score',
            'home_1': 'home_home_1', 'away_1': 'home_away_1',
            'home_2': 'home_home_2', 'away_2': 'home_away_2',
            'home_3': 'home_home_3', 'away_3': 'home_away_3',
            'home_4': 'home_home_4', 'away_4': 'home_away_4',
            'home_5': 'home_home_5', 'away_5': 'home_away_5',
            'home_6': 'home_home_6', 'away_6': 'home_away_6',
            'home_7': 'home_home_7', 'away_7': 'home_away_7',
            'home_8': 'home_home_8', 'away_8': 'home_away_8',
            'home_9': 'home_home_9', 'away_9': 'home_away_9',
        }
        df_finished_games = df_finished_games.rename(columns=daily_csv_column_mapping)
        print("Columns renamed to match master template.")

        print("\n--- Performing data type conversions and derivations for finished daily data ---")

        # Correctly combine game_date_et_temp and start_time_et_str into a single timezone-aware datetime
        if 'game_date_et_temp' in df_finished_games.columns and 'start_time_et_str' in df_finished_games.columns:
            df_finished_games['game_date_et'] = pd.to_datetime(df_finished_games['game_date_et_temp'], errors='coerce')
            df_finished_games['start_time_et'] = df_finished_games.apply(
                lambda row: (
                    eastern.localize(datetime.strptime(f"{row['game_date_et'].strftime('%Y-%m-%d')} {row['start_time_et_str'].replace(' ET', '')}", '%Y-%m-%d %H:%M'))
                    if pd.notna(row['game_date_et']) and pd.notna(row['start_time_et_str'])
                    else pd.NaT
                ), axis=1
            )
            print("-> Combined 'game_date' and 'start_time_et' to create timezone-aware 'game_date_et' and 'start_time_et' columns.")
            df_finished_games = df_finished_games.drop(columns=['game_date_et_temp', 'start_time_et_str'])
        else:
            print("Warning: 'game_date' or 'start_time_et' columns not found in daily data for proper datetime parsing. Skipping datetime conversion for these columns.")

        # Handle other datetime columns
        for col_name in ['game_date', 'commence_time']:
            if col_name in df_finished_games.columns:
                df_finished_games[col_name] = pd.to_datetime(df_finished_games[col_name], errors='coerce')
                if df_finished_games[col_name].dt.tz is None:
                    df_finished_games[col_name] = df_finished_games[col_name].dt.tz_localize(eastern)
                else:
                    df_finished_games[col_name] = df_finished_games[col_name].dt.tz_convert(eastern)

        # Convert specific numeric columns to Int64 (nullable integer)
        nullable_int_cols = [
            'home_home_score', 'home_away_score', 'home_Wins', 'home_Losses',
            'home_Win_Streak', 'home_Loss_Streak', 'game_id_odds', 'home_run_diff', 'season',
            'home_Games_Played', 'away_home_score', 'away_away_score', 'away_Wins', 'away_Losses',
            'away_Win_Streak', 'away_Loss_Streak', 'away_run_diff', 'away_Games_Played',
            'run_diff'
        ]
        for i in range(1, 10):
            nullable_int_cols.extend([f'home_home_{i}', f'home_away_{i}'])

        for col in nullable_int_cols:
            if col in df_finished_games.columns:
                df_finished_games[col] = pd.to_numeric(df_finished_games[col], errors='coerce').astype(pd.Int64Dtype())

        # Ensure float columns are float64
        float_cols = [
            'home_Win_Pct', 'home_team_odds', 'home_opponent_odds', 'home_Run_Line',
            'home_Spread_Price', 'home_Opp_Spread_Price', 'home_Total', 'home_Over_Price',
            'home_Under_Price', 'home_h2h_own', 'home_h2h_opp', 'away_Win_Pct',
            'away_team_odds', 'away_opponent_odds', 'away_Run_Line', 'away_Spread_Price',
            'away_Opp_Spread_Price', 'away_Total', 'away_Over_Price', 'away_Under_Price',
            'away_h2h_own', 'away_h2h_opp'
        ]
        for col in float_cols:
            if col in df_finished_games.columns:
                df_finished_games[col] = pd.to_numeric(df_finished_games[col], errors='coerce').astype(float)

        # Derivations
        if 'game_date_et' in df_finished_games.columns:
            df_finished_games['season'] = df_finished_games['game_date_et'].dt.year.astype(pd.Int64Dtype())
            print("-> Derived 'season' from 'game_date_et'.")

        # Check for original columns before renaming/dropping for winner/total_result
        # The 'winner' and 'total_result' columns are from the original daily CSV.
        # Need to ensure we're referencing them before `df_daily` is out of scope or significantly altered.
        # Assuming `df_daily` still holds the original structure at this point or these derivations
        # happen before dropping the original columns if they are not mapped.
        # If 'winner' and 'total_result' were among the columns mapped/renamed, these checks are problematic.
        # Re-check the source of 'winner' and 'total_result' to confirm.
        # For robustness, we will assume they *were not* renamed to something else in daily_csv_column_mapping.
        # If they *were* renamed and thus not present, these lines will add NaN columns.
        if 'winner' in df_daily.columns:
            df_finished_games['home_won_game'] = df_daily['winner'].apply(lambda x: True if x == 'Home' else (False if x == 'Away' else pd.NA)).astype(pd.BooleanDtype())
            df_finished_games['away_won_game'] = df_daily['winner'].apply(lambda x: True if x == 'Away' else (False if x == 'Home' else pd.NA)).astype(pd.BooleanDtype())
            print("-> Derived 'home_won_game' and 'away_won_game' from 'winner'.")
        else:
            df_finished_games['home_won_game'] = pd.Series(dtype=pd.BooleanDtype())
            df_finished_games['away_won_game'] = pd.Series(dtype=pd.BooleanDtype())
            print("Warning: 'winner' column not found in daily data. 'home_won_game' and 'away_won_game' set to NA.")


        if 'total_result' in df_daily.columns:
            df_finished_games['home_hit_over'] = df_daily['total_result'].apply(lambda x: True if x == 'Over' else (False if x == 'Under' else pd.NA)).astype(pd.BooleanDtype())
            df_finished_games['away_hit_over'] = df_daily['total_result'].apply(lambda x: True if x == 'Over' else (False if x == 'Under' else pd.NA)).astype(pd.BooleanDtype())
            print("-> Derived 'home_hit_over' and 'away_hit_over' from 'total_result'.")
        else:
            df_finished_games['home_hit_over'] = pd.Series(dtype=pd.BooleanDtype())
            df_finished_games['away_hit_over'] = pd.Series(dtype=pd.BooleanDtype())
            print("Warning: 'total_result' column not found in daily data. 'home_hit_over' and 'away_hit_over' set to NA.")


        # Derive game_date_for_merge
        if 'game_date_et' in df_finished_games.columns:
            df_finished_games['game_date_for_merge'] = df_finished_games['game_date_et']
            print("-> Derived 'game_date_for_merge' from 'game_date_et'.")

        print("\nAdding missing columns (from master schema) to daily data...")
        for col, dtype in master_schema.items():
            if col not in df_finished_games.columns:
                if 'Int64' in str(dtype):
                    df_finished_games[col] = pd.Series(dtype=pd.Int64Dtype())
                elif 'float' in str(dtype):
                    df_finished_games[col] = np.nan
                elif 'boolean' in str(dtype):
                    df_finished_games[col] = pd.Series(dtype=pd.BooleanDtype())
                elif 'datetime' in str(dtype):
                    tz_str = None
                    if '[' in str(dtype) and ']' in str(dtype):
                        bracket_content = str(dtype).split('[')[1].split(']')[0]
                        parts = [p.strip() for p in bracket_content.split(',')]
                        if len(parts) > 1:
                            tz_str = parts[1]
                    if tz_str:
                        df_finished_games[col] = pd.Series(dtype='datetime64[ns]').dt.tz_localize(tz_str)
                    else:
                        df_finished_games[col] = pd.Series(dtype='datetime64[ns]')
                else:
                    df_finished_games[col] = pd.Series(dtype=object)

        # Drop columns from daily data that are not in the master schema
        cols_to_drop = [col for col in df_finished_games.columns if col not in master_schema]
        if cols_to_drop:
            print(f"Dropped columns from daily data not in master schema: {cols_to_drop}")
            df_finished_games = df_finished_games.drop(columns=cols_to_drop)

        # Ensure all columns are of the correct type (including newly added ones)
        for col, dtype in master_schema.items():
            if col in df_finished_games.columns:
                try:
                    if dtype == 'Int64':
                        df_finished_games[col] = pd.to_numeric(df_finished_games[col], errors='coerce').astype(pd.Int64Dtype())
                    elif dtype == 'boolean':
                        df_finished_games[col] = df_finished_games[col].astype(pd.BooleanDtype())
                    elif 'datetime' in str(dtype):
                        df_finished_games[col] = pd.to_datetime(df_finished_games[col], errors='coerce')
                        if 'tz' in str(dtype):
                            target_tz_str = str(dtype).split('[')[1].split(']')[0].split(', ')[1]
                            target_tz = pytz.timezone(target_tz_str)
                            if df_finished_games[col].dt.tz is None:
                                df_finished_games[col] = df_finished_games[col].dt.tz_localize(target_tz)
                            else:
                                df_finished_games[col] = df_finished_games[col].dt.tz_convert(target_tz)
                        else:
                            if df_finished_games[col].dt.tz is not None:
                                df_finished_games[col] = df_finished_games[col].dt.tz_convert(None)
                    elif dtype == 'float64':
                         df_finished_games[col] = pd.to_numeric(df_finished_games[col], errors='coerce').astype(float)
                    else:
                        df_finished_games[col] = df_finished_games[col].astype(dtype)

                except Exception as e:
                    print(f"Error casting newly added column '{col}' to {dtype}: {e}")

        # Reorder columns to match the master schema explicitly
        df_finished_games = df_finished_games[list(master_schema.keys())]
        print("Daily DataFrame columns aligned and reordered to master schema.")

        print("\nFinished Daily DataFrame dtypes after conversion and alignment:")
        df_finished_games.info()

        # --- Append to Master File ---
        print("\n--- Appending finished daily data to master file ---")
        df_master = pd.DataFrame()
        if os.path.exists(MASTER_FILE) and os.path.getsize(MASTER_FILE) > 0:
            df_master = pd.read_parquet(MASTER_FILE)
            print(f"Existing master file loaded. Rows: {len(df_master)}")
            # Ensure existing master file has all columns from the current master_schema
            for col, dtype in master_schema.items():
                if col not in df_master.columns:
                    if 'Int64' in str(dtype):
                        df_master[col] = pd.Series(dtype=pd.Int64Dtype())
                    elif 'float' in str(dtype):
                        df_master[col] = np.nan
                    elif 'boolean' in str(dtype):
                        df_master[col] = pd.Series(dtype=pd.BooleanDtype())
                    elif 'datetime' in str(dtype):
                        tz_str = None
                        if '[' in str(dtype) and ']' in str(dtype):
                            bracket_content = str(dtype).split('[')[1].split(']')[0]
                            parts = [p.strip() for p in bracket_content.split(',')]
                            if len(parts) > 1:
                                tz_str = parts[1]
                        if tz_str:
                            df_master[col] = pd.Series(dtype='datetime64[ns]').dt.tz_localize(tz_str)
                        else:
                            df_master[col] = pd.Series(dtype='datetime64[ns]')
                    else:
                        df_master[col] = pd.Series(dtype=object)

            # Ensure master file also has correct dtypes after potentially adding columns
            for col, dtype in master_schema.items():
                if col in df_master.columns:
                    try:
                        if dtype == 'Int64':
                            df_master[col] = pd.to_numeric(df_master[col], errors='coerce').astype(pd.Int64Dtype())
                        elif dtype == 'boolean':
                            df_master[col] = df_master[col].astype(pd.BooleanDtype())
                        elif 'datetime' in str(dtype):
                            df_master[col] = pd.to_datetime(df_master[col], errors='coerce')
                            if 'tz' in str(dtype):
                                target_tz_str = str(dtype).split('[')[1].split(']')[0].split(', ')[1]
                                target_tz = pytz.timezone(target_tz_str)
                                if df_master[col].dt.tz is None:
                                    df_master[col] = df_master[col].dt.tz_localize(target_tz)
                                else:
                                    df_master[col] = df_master[col].dt.tz_convert(target_tz)
                            else:
                                if df_master[col].dt.tz is not None:
                                    df_master[col] = df_master[col].dt.tz_convert(None)
                        elif dtype == 'float64':
                             df_master[col] = pd.to_numeric(df_master[col], errors='coerce').astype(float)
                        else:
                            df_master[col] = df_master[col].astype(dtype)
                    except Exception as e:
                        print(f"Warning: Could not cast existing master column '{col}' to {dtype}. Error: {e}")
            print("Existing master file aligned to new schema.")

        # Align dtypes between daily and master before concatenation
        print("Aligning dtypes between daily and master DataFrames for concat...")
        for col, master_dtype in master_schema.items():
            if col in df_finished_games.columns and col in df_master.columns:
                daily_dtype = df_finished_games[col].dtype
                if daily_dtype != master_dtype:
                    try:
                        if master_dtype == 'Int64':
                            df_finished_games[col] = df_finished_games[col].astype(master_dtype)
                        elif master_dtype == 'boolean':
                            df_finished_games[col] = df_finished_games[col].astype(master_dtype)
                        elif 'datetime' in str(master_dtype):
                            if df_finished_games[col].dt.tz is None and 'tz' in str(master_dtype):
                                target_tz_str = str(master_dtype).split('[')[1].split(']')[0].split(', ')[1]
                                df_finished_games[col] = df_finished_games[col].dt.tz_localize(target_tz_str)
                            elif df_finished_games[col].dt.tz is not None and 'tz' not in str(master_dtype):
                                df_finished_games[col] = df_finished_games[col].dt.tz_convert(None)
                            elif df_finished_games[col].dt.tz is not None and 'tz' in str(master_dtype):
                                target_tz_str = str(master_dtype).split('[')[1].split(']')[0].split(', ')[1]
                                df_finished_games[col] = df_finished_games[col].dt.tz_convert(target_tz_str)
                        elif master_dtype == 'float64':
                             df_finished_games[col] = pd.to_numeric(df_finished_games[col], errors='coerce').astype(float)
                        else:
                            df_finished_games[col] = df_finished_games[col].astype(master_dtype)

                    except Exception as e:
                        print(f"Warning: Could not cast daily '{col}' ({daily_dtype}) to master's target '{col}' ({master_dtype}). Error: {e}")

        # Combine dataframes
        df_combined = pd.concat([df_master, df_finished_games], ignore_index=True)

        # Drop duplicates
        initial_rows = len(df_combined)
        df_combined.drop_duplicates(subset=['game_id', 'game_date_et', 'home_team', 'home_opponent'], inplace=True)
        duplicates_removed = initial_rows - len(df_combined)
        print(f"Removed {duplicates_removed} duplicate rows during merge.")
        print(f"New total rows in combined DataFrame: {len(df_combined)}")

        # Sort final DataFrame by game_date_et and game_id
        print("Sorting final DataFrame...")
        df_combined.sort_values(by=['game_date_et', 'game_id'], inplace=True)
        print("Sorting complete.")

        # Save to parquet
        df_combined.to_parquet(MASTER_FILE, index=False)
        print(f"✅ Master file updated successfully with finished daily data from {os.path.basename(file_path)}.")
        print(f"Final master file rows: {len(df_combined)}")

    except Exception as e:
        print(f"❌ An error occurred during processing {file_path}: {e}")

    print("\n--- Daily Data Appending to Master Complete ---")


def backfill_missing_data(start_date_str: str, end_date_str: str):
    """
    Backfills missing MLB betting data for a specified date range.
    """
    print(f"\n--- Starting backfill process from {start_date_str} to {end_date_str} ---")
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    current_date = start_date
    while current_date <= end_date:
        file_date_str = current_date.strftime("%Y-%m-%d")
        daily_file_path = os.path.join(DAILY_DATA_DIR, f"MLB_Combined_Odds_Results_{file_date_str}.csv")
        
        process_daily_file(daily_file_path) # Call the single file processing function

        current_date += timedelta(days=1)
    print(f"\n--- Backfill process from {start_date_str} to {end_date_str} complete ---")


# --- Main execution logic ---
if __name__ == "__main__":
    # 1. Run the daily update for yesterday's data
    yesterday = datetime.now(eastern) - timedelta(days=1)
    yesterday_file = os.path.join(DAILY_DATA_DIR, f"MLB_Combined_Odds_Results_{yesterday.strftime('%Y-%m-%d')}.csv")
    
    # Create a dummy daily file for testing if it doesn't exist (remove for production)
    # This block is for testing purposes only to ensure the script runs without FileNotFoundError
    if not os.path.exists(yesterday_file):
        print(f"Creating a dummy daily file for {yesterday.strftime('%Y-%m-%d')} for testing purposes.")
        # Create a minimal dummy CSV with required columns for a "Finished" game
        dummy_data = {
            'game_id': [12345],
            'game_date': [yesterday.strftime('%Y-%m-%d')],
            'start_time_et': ['19:05 ET'],
            'home_team': ['Team A'],
            'away_team': ['Team B'],
            'moneyline_home': [-150],
            'moneyline_away': [130],
            'total_line': [8.5],
            'over_odds': [-110],
            'under_odds': [-110],
            'home_score': [5],
            'away_score': [3],
            'status': ['Finished'],
            'winner': ['Home'],
            'total_result': ['Over'],
            'home_1': [0], 'away_1': [1],
            'home_2': [2], 'away_2': [0],
            'home_3': [1], 'away_3': [1],
            'home_4': [0], 'away_4': [0],
            'home_5': [1], 'away_5': [0],
            'home_6': [0], 'away_6': [0],
            'home_7': [1], 'away_7': [0],
            'home_8': [0], 'away_8': [1],
            'home_9': [0], 'away_9': [0],
        }
        dummy_df = pd.DataFrame(dummy_data)
        dummy_df.to_csv(yesterday_file, index=False)
        print(f"Dummy file created at: {yesterday_file}")

    process_daily_file(yesterday_file)

    # 2. Backfill missing data for the specified range
    backfill_missing_data("2025-06-09", "2025-06-23")

    print("\n--- Final Master File Info (after all updates and backfills) ---")
    if os.path.exists(MASTER_FILE):
        final_master_df = pd.read_parquet(MASTER_FILE)
        print(f"Total rows in master file: {len(final_master_df)}")
        print(final_master_df.head())
        print("\nMaster File Dtypes:")
        final_master_df.info()
    else:
        print("Master file does not exist after all operations.")
