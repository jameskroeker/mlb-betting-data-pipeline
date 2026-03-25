# scripts/archive_old_files.py

import os
import shutil
from datetime import datetime, timedelta

# === CHANGED: Dynamic year — never needs updating again ===
CURRENT_YEAR = datetime.now().year

# === Config ===
daily_dir = "data/daily"
archive_dir = f"data/archive/MLB/{CURRENT_YEAR}"
os.makedirs(archive_dir, exist_ok=True)

# === Cutoff: 7 days ago ===
cutoff_date = datetime.today() - timedelta(days=7)

moved_files = []

# === CHANGED: File prefix uses dynamic year instead of hardcoded 2025 ===
file_prefix = f"MLB_Combined_Odds_Results_{CURRENT_YEAR}"

for filename in os.listdir(daily_dir):
    if filename.startswith(file_prefix) and filename.endswith(".csv"):
        try:
            date_str = filename.split("_")[-1].replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            if file_date < cutoff_date:
                src_path = os.path.join(daily_dir, filename)
                dest_path = os.path.join(archive_dir, filename)
                shutil.move(src_path, dest_path)
                moved_files.append(filename)
        except Exception as e:
            print(f"⚠️ Could not process {filename}: {e}")

# === Report ===
if moved_files:
    print(f"📦 Archived {len(moved_files)} file(s) to {archive_dir} (older than 7 days):")
    for f in moved_files:
        print(f"  - {f}")
else:
    print(f"✅ No {CURRENT_YEAR} files older than 7 days found in {daily_dir}.")
