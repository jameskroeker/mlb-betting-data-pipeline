#!/usr/bin/env python3
# scripts/update_signal_results.py
# Runs after the daily pipeline + lock (e.g. 9:30 UTC) — backfills W/L results
# into past signal lock files by joining game_id + signal_team against the
# master parquet's team_won field. Idempotent: re-running just overwrites
# results with the latest data, safe to run every day.

import os
import json
import glob
import pandas as pd

SIGNALS_DIR = "data/signals"
MASTER_PARQUET = "data/master/master_template.parquet"

def main():
    if not os.path.exists(MASTER_PARQUET):
        print(f"❌ Master parquet not found at {MASTER_PARQUET}")
        return False

    master_df = pd.read_parquet(MASTER_PARQUET)
    # Index for fast lookup: (game_id, team_abbr) -> team_won
    lookup = {}
    for _, row in master_df[['game_id', 'team_abbr', 'team_won']].iterrows():
        lookup[(int(row['game_id']), row['team_abbr'])] = bool(row['team_won'])

    signal_files = sorted(glob.glob(os.path.join(SIGNALS_DIR, "signals_*.json")))
    print(f"Found {len(signal_files)} signal lock files to check")

    updated_files = 0
    total_filled = 0

    for path in signal_files:
        with open(path) as f:
            data = json.load(f)

        changed = False
        for sig in data.get("signals", []):
            game_id = sig.get("game_id")
            team = sig.get("signal_team")
            try:
                game_id_int = int(game_id)
            except (TypeError, ValueError):
                continue

            key = (game_id_int, team)
            if key in lookup:
                new_result = "W" if lookup[key] else "L"
                if sig.get("result") != new_result:
                    sig["result"] = new_result
                    changed = True
                    total_filled += 1
            else:
                # Game not finished yet, or not in master — leave as-is (pending)
                if "result" not in sig:
                    sig["result"] = None

        if changed:
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            updated_files += 1
            print(f"✅ Updated {os.path.basename(path)}")

    print(f"\nDone. {updated_files} file(s) updated, {total_filled} result(s) filled/changed.")
    return True

if __name__ == "__main__":
    main()
