import os
import shutil
from datetime import datetime, timedelta

# === Config ===
daily_dir = "data/daily"
archive_dir = "data/archive/MLB/2025"
os.makedirs(archive_dir, exist_ok=True)

# === Cutoff: 7 days ago
cutoff_date = datetime.today() - timedelta(days=7)
moved_files = []

# === Move old files
for filename in os.listdir(daily_dir):
    if filename.startswith("MLB_Combined_Odds_Results_2025") and filename.endswith(".csv"):
        try:
            date_str = filename.split("_")[-1].replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            if file_date < cutoff_date:
                src_path = os.path.join(daily_dir, filename)
                dest_path = os.path.join(archive_dir, filename)
                shutil.move(src_path, dest_path)  # 🔁 move = remove + store
                moved_files.append(filename)
        except Exception as e:
            print(f"⚠️ Could not process {filename}: {e}")

# === Report
if moved_files:
    print(f"📦 Moved {len(moved_files)} file(s) to archive (older than 7 days):")
    for f in moved_files:
        print(f"  - {f}")
else:
    print("✅ No files older than 7 days found in data/daily.")
