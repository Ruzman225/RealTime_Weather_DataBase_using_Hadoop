#!/usr/bin/env python
"""Mapper: city|YYYY-MM -> daily precipitation amount in mm."""
import json
import sys

for line in sys.stdin:
    try:
        row = json.loads(line)
        city = str(row.get("city", "")).strip()
        day = str(row.get("weather_time", ""))[:10]
        value = row.get("precipitation_sum_mm", row.get("precipitation_mm"))
        # This job is intended for historical daily records. Do not interpret a
        # minute-by-minute current-condition sample as a daily rainfall total.
        if row.get("record_type") != "historical_daily":
            continue
        if not city or len(day) < 7 or value is None:
            continue
        month = day[:7]
        precipitation = float(value)
        print(f"{city}|{month}\t{precipitation}")
    except (ValueError, TypeError, json.JSONDecodeError):
        continue
