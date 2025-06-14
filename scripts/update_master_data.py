# scripts/update_master_data.py

import pandas as pd
import os
from datetime import datetime, timedelta
import pytz

# === Config ===
MASTER_FILE = "data/master/master_template.parquet" # Path relative to repo root
eastern = pytz.timezone("US/Eastern")

# We want to process yesterday's file, as its games should be finished and enriched by now
process_date_str = (datetime.now(eastern) - timedelta(days=1)).strftime("%Y-%m-%d")
daily_csv_to_process = f"data/daily/MLB_Combined_Odds_Results_{process_date_str}.csv"

print(f"\n--- Attempting to process completed game data from {daily_csv_to_process} ---")

if not os.path.exists(daily_csv_to_process):
    print(f"❌ Error: Daily CSV for {process_date_str} not found at {daily_csv_to_process}. Skipping append to master.")
else:
    try:
        df_daily = pd.read_csv(daily_csv_to_process, low_memory=False)
        print(f"✅ Daily CSV loaded successfully. Rows: {len(df_daily)}")

        # --- Filter for FINISHED games only before processing ---
        df_finished_games = df_daily[df_daily['status'] == 'Finished'].copy()

        if df_finished_games.empty:
            print(f"⚠️ No 'Finished' games found in {daily_csv_to_process}. Nothing to append to master.")
        else:
            print(f"Found {len(df_finished_games)} finished games to process.")

            # --- Apply Data Type Conversions (as refined previously) ---
            print("\n--- Performing data type conversions for finished daily data ---")

            date_cols = ['game_date', 'start_time_et']
            for col in date_cols:
                if col in df_finished_games.columns:
                    df_finished_games[col] = pd.to_datetime(df_finished_games[col], errors='coerce', utc=True).dt.tz_convert(eastern)
                else:
                    print(f"Warning: Date column '{col}' not found in daily data for conversion.")

            numeric_cols = [
                'game_id', 'moneyline_home', 'moneyline_away', 'total_line', 'over_odds', 'under_odds',
                'home_score', 'away_score',
                'home_1', 'away_1', 'home_2', 'away_2', 'home_3', 'away_3',
                'home_4', 'away_4', 'home_5', 'away_5', 'home_6', 'away_6',
                'home_7', 'away_7', 'home_8', 'away_8', 'home_9', 'away_9'
            ]
            for col in numeric_cols:
                if col in df_finished_games.columns:
                    df_finished_games[col] = pd.to_numeric(df_finished_games[col], errors='coerce')
                else:
                    print(f"Warning: Numeric column '{col}' not found in daily data for conversion.")

            object_cols = ['home_team', 'away_team', 'status', 'winner', 'total_result']
            for col in object_cols:
                if col in df_finished_games.columns:
                    df_finished_games[col] = df_finished_games[col].astype(str)
                    df_finished_games[col] = df_finished_games[col].replace('nan', None) # Replace string 'nan' with None
                else:
                    print(f"Warning: Object column '{col}' not found in daily data for conversion.")

            if 'season' not in df_finished_games.columns:
                df_finished_games['season'] = df_finished_games['game_date'].dt.year


            print("\nFinished Daily DataFrame dtypes after conversion:")
            print(df_finished_games.info())

            # --- Load Master File and Append ---
            print("\n--- Appending finished daily data to master file ---")

            df_master = pd.DataFrame() # Initialize as empty
            if os.path.exists(MASTER_FILE):
                try:
                    df_master = pd.read_parquet(MASTER_FILE)
                    print(f"Existing master file loaded. Rows: {len(df_master)}")
                except Exception as e:
                    print(f"❌ Error loading existing master file: {e}. Starting with an empty DataFrame.")
            else:
                print("Master file does not exist, creating a new one.")


            # Align dtypes between daily and master DataFrames before concat
            print("Aligning dtypes between daily and master DataFrames for concat...")
            for col in df_finished_games.columns:
                if col in df_master.columns:
                    if df_master[col].dtype != df_finished_games[col].dtype:
                        try:
                            # Attempt to cast daily data to master's dtype
                            df_finished_games[col] = df_finished_games[col].astype(df_master[col].dtype)
                        except Exception as e:
                            print(f"Warning: Could not cast daily '{col}' ({df_finished_games[col].dtype}) to master's '{col}' ({df_master[col].dtype}). Error: {e}")
                elif col in ['game_date', 'start_time_et'] and not df_master.empty:
                    # Special handling for datetime columns if they become the first of their type
                    # Ensure timezone-aware if master is timezone-aware
                    if df_master['game_date'].dt.tz is not None:
                        df_finished_games[col] = df_finished_games[col].dt.tz_convert(eastern)


            combined_df = pd.concat([df_master, df_finished_games], ignore_index=True)

            # --- Deduplication ---
            # Use a robust key that identifies a unique game record
            # Adjust if your master_template.parquet has other unique identifiers like 'team_abbr'
            combined_df['dedup_key'] = combined_df['game_id'].astype(str) + '_' + \
                                       combined_df['game_date'].dt.strftime('%Y-%m-%d').astype(str) + '_' + \
                                       combined_df['home_team'].astype(str) + '_' + \
                                       combined_df['away_team'].astype(str)

            rows_before_dedup = len(combined_df)
            combined_df.drop_duplicates(subset=['dedup_key'], keep='first', inplace=True)
            rows_after_dedup = len(combined_df)
            combined_df.drop(columns=['dedup_key'], inplace=True) # Remove the temporary key

            print(f"Removed {rows_before_dedup - rows_after_dedup} duplicate rows during merge.")
            print(f"New total rows in combined DataFrame: {len(combined_df)}")

            print("Sorting final DataFrame...")
            # Sorting helps ensure consistent output order and can aid in debugging
            combined_df = combined_df.sort_values(by=['season', 'game_date', 'game_id', 'home_team']).reset_index(drop=True)
            print("Sorting complete.")

            combined_df.to_parquet(MASTER_FILE, index=False)
            print(f"✅ Master file updated successfully with finished daily data: {MASTER_FILE}")
            print(f"Final master file rows: {len(combined_df)}")

            print("\n--- Final Row Counts per Season in Master File ---")
            season_counts = combined_df['season'].value_counts().sort_index()
            print(season_counts)
            print("\n--- Daily Data Appending to Master Complete ---")

        except pd.errors.EmptyDataError:
            print(f"⚠️ Daily CSV for {process_date_str} is empty. No finished games to append.")
        except Exception as e:
            print(f"❌ An error occurred during daily data ingestion to master: {e}")
