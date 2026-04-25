#!/usr/bin/env python3
# scripts/lock_signals.py
# Runs at 8PM ET (00:00 UTC) — saves today's T1 signals by calling the live backend API

import os
import json
import requests
from datetime import datetime, timedelta
import pytz

eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)

utc_hour = datetime.utcnow().hour
if utc_hour not in (0, 1) and os.environ.get("FORCE_LOCK") != "1":
    print(f"Not 8PM ET run (UTC hour={utc_hour}) — skipping signal lock")
    exit(0)

target_date = (now_et - timedelta(hours=4)).strftime("%Y-%m-%d")
if os.environ.get("FORCE_LOCK") == "1":
    target_date = now_et.strftime("%Y-%m-%d")

print(f"Locking signals for {target_date}")

output_path = f"data/signals/signals_{target_date}.json"
os.makedirs("data/signals", exist_ok=True)

if os.path.exists(output_path) and os.environ.get("FORCE_LOCK") != "1":
    print(f"Signal file already exists: {output_path} — skipping")
    exit(0)

BACKEND_URL = os.environ.get("BACKEND_URL", "https://strikes-and-downs.onrender.com")

print(f"Fetching signals from {BACKEND_URL}/api/signals/{target_date}")
resp = requests.get(f"{BACKEND_URL}/api/signals/{target_date}", timeout=60)
resp.raise_for_status()
data = resp.json()

t1_signals = [g for g in data.get("signals", []) if g.get("tier") == 1]

output = {
    "date": target_date,
    "locked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "t1_count": len(t1_signals),
    "signals": [{
        "game_id": g["game_id"],
        "game_date": target_date,
        "home_team": g["home_team"],
        "away_team": g["away_team"],
        "signal_team": g["signal_team"],
        "consensus_score": g["consensus_score"],
        "tier": 1,
    } for g in t1_signals],
}

with open(output_path, "w") as f:
    json.dump(output, f, indent=2)

for s in output["signals"]:
    print(f"  T1: {s['away_team']} @ {s['home_team']} | signal={s['signal_team']} | score={s['consensus_score']}")

print(f"\nLocked {len(t1_signals)} T1 signals to {output_path}")
