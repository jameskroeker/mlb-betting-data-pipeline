import os
from datetime import datetime, timedelta

# === Config ===
folder = "data/daily"
start_date = datetime(2025, 5, 2)
end_date = datetime.today()

missing = []

for i in range((end_date - start_date).days + 1):
    day = start_date + timedelta(days=i)
    filename = f"MLB_Combined_Odds_Results_{day.strftime('%Y-%m-%d')}.csv"
    full_path = os.path.join(folder, filename)

    if not os.path.exists(full_path):
        missing.append(filename)

if missing:
    print("❌ Missing daily files:")
    for f in missing:
        print("  -", f)
else:
    print("✅ All expected files are present!")
