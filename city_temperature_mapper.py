
#!/usr/bin/env python
import json
import sys

for line in sys.stdin:
    try:
        row = json.loads(line)

        city = str(row.get("city", "")).strip()

        mean_temp = row.get(
            "temperature_mean_c",
            row.get("temperature_2m_mean", row.get("temperature_c"))
        )

        min_temp = row.get(
            "temperature_min_c",
            row.get("temperature_2m_min")
        )

        max_temp = row.get(
            "temperature_max_c",
            row.get("temperature_2m_max")
        )

        if not city or mean_temp is None:
            continue

        mean_temp = float(mean_temp)
        min_temp = float(min_temp) if min_temp is not None else mean_temp
        max_temp = float(max_temp) if max_temp is not None else mean_temp

        print(f"{city}\t{mean_temp},{min_temp},{max_temp},1")

    except (ValueError, TypeError, json.JSONDecodeError):
        continue
