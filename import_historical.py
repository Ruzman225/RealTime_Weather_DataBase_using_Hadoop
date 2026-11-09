"""Import daily historical weather data for all configured Indian cities to HDFS.

Default range: 2020-01-01 through 2025-12-31.
Example: python import_historical.py --start 2020-01-01 --end 2025-12-31

The script stores one JSON object per city/date (JSON Lines) and uploads the
files into HDFS. Historical values are model/reanalysis data from Open-Meteo.
"""
import argparse
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from locations import LOCATIONS

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "historical_cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
HDFS_ROOT = "/weather/historical/raw"
TIMEZONE = ZoneInfo("Asia/Kolkata")
BATCH_CITIES = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def request_with_retries(url, *, params, timeout=90, attempts=4):
    last_error = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code in (429, 500, 502, 503, 504) and attempt < attempts - 1:
                wait = min(2 ** attempt, 16)
                logging.warning("Historical API returned HTTP %s; retrying in %s seconds", response.status_code, wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == attempts - 1:
                break
            wait = min(2 ** attempt, 16)
            logging.warning("Historical request failed: %s; retrying in %s seconds", exc, wait)
            time.sleep(wait)
    raise last_error


def find_hdfs_cmd():
    candidates = []
    if os.environ.get("HADOOP_HOME"):
        candidates.append(Path(os.environ["HADOOP_HOME"]) / "bin" / "hdfs.cmd")
    for name in ("hdfs.cmd", "hdfs.bat", "hdfs.exe"):
        found = shutil.which(name)
        if found:
            candidates.append(Path(found))
    candidates.extend([
        Path(r"C:\Hadoop\bin\hdfs.cmd"),
        Path(r"C:\College\Applications downloads\hadoop-3.2.4\hadoop-3.2.4\bin\hdfs.cmd"),
    ])
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("Cannot find hdfs.cmd. Set HADOOP_HOME or fix the path in import_historical.py.")


def _quote_cmd_arg(value):
    """Quote each argument for Windows cmd.exe, including HDFS paths."""
    value = str(value)
    return '"' + value.replace('"', '""') + '"'


def run_hdfs(args, timeout=300):
    import subprocess
    import os

    hdfs_cmd = r"C:\Hadoop\bin\hdfs.cmd"

    command = (
        f'"{hdfs_cmd}" '
        + subprocess.list2cmdline([str(x) for x in args])
    )

    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=os.environ.copy()
    )

    if result.returncode != 0:
        details = (result.stdout or "") + "\n" + (result.stderr or "")
        raise RuntimeError(
            f"HDFS command failed: {command}\n{details}"
        )

    return result


def import_group(start_date, end_date, names, group_number):
    params = {
        "latitude": ",".join(str(LOCATIONS[name][0]) for name in names),
        "longitude": ",".join(str(LOCATIONS[name][1]) for name in names),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,relative_humidity_2m_mean,wind_speed_10m_max",
        "timezone": "Asia/Kolkata",
    }
    response = request_with_retries(ARCHIVE_API, params=params)
    payload = response.json()
    results = payload if isinstance(payload, list) else [payload]
    if len(results) != len(names):
        raise ValueError(f"Expected {len(names)} historical location responses; got {len(results)}")

    imported_at = datetime.now(TIMEZONE).isoformat()
    local_file = DATA_DIR / f"historical_daily_{start_date:%Y%m%d}_{end_date:%Y%m%d}_group_{group_number:02d}.jsonl"
    count = 0
    with local_file.open("w", encoding="utf-8", newline="\n") as handle:
        for name, result in zip(names, results):
            daily = result.get("daily") or {}
            dates = daily.get("time") or []
            arrays = {
                "temperature_mean_c": daily.get("temperature_2m_mean") or [],
                "temperature_max_c": daily.get("temperature_2m_max") or [],
                "temperature_min_c": daily.get("temperature_2m_min") or [],
                "precipitation_sum_mm": daily.get("precipitation_sum") or [],
                "humidity_mean_pct": daily.get("relative_humidity_2m_mean") or [],
                "wind_speed_max_kmh": daily.get("wind_speed_10m_max") or [],
            }
            lat, lon = LOCATIONS[name]
            for i, day in enumerate(dates):
                record = {
                    "record_type": "historical_daily",
                    "source": "Open-Meteo Historical Weather API",
                    "city": name,
                    "latitude": lat,
                    "longitude": lon,
                    "weather_time": day,
                    "collected_at": imported_at,
                }
                for output_key, values in arrays.items():
                    record[output_key] = values[i] if i < len(values) else None
                # Preserve convenience fields used by shared reporting code.
                record["temperature_c"] = record["temperature_mean_c"]
                record["precipitation_mm"] = record["precipitation_sum_mm"]
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1

    # Use a plain path segment (no key=value partition notation) for maximum
    # compatibility with this Windows Hadoop shell setup.
    hdfs_dir = f"{HDFS_ROOT}/{start_date.isoformat()}_to_{end_date.isoformat()}"
    run_hdfs(["dfs", "-mkdir", "-p", hdfs_dir])
    run_hdfs(["dfs", "-put", "-f", str(local_file), hdfs_dir], timeout=300)
    hdfs_path = f"{hdfs_dir}/{local_file.name}"
    logging.info("Imported %s records to %s", count, hdfs_path)
    return count


def main():
    parser = argparse.ArgumentParser(description="Import daily historical weather records to HDFS.")
    parser.add_argument("--start", default="2020-01-01", help="Start date YYYY-MM-DD (default: 2020-01-01)")
    parser.add_argument("--end", default="2025-12-31", help="End date YYYY-MM-DD (default: 2025-12-31)")
    parser.add_argument("--batch-cities", type=int, default=BATCH_CITIES, help="Locations per API request (default: 5)")
    args = parser.parse_args()
    try:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
    except ValueError as exc:
        parser.error(f"Dates must be YYYY-MM-DD: {exc}")
    if end_date < start_date:
        parser.error("--end must be on or after --start")
    if not 1 <= args.batch_cities <= 10:
        parser.error("--batch-cities must be between 1 and 10")

    names = list(LOCATIONS)
    total = 0
    total_groups = (len(names) + args.batch_cities - 1) // args.batch_cities
    for index, offset in enumerate(range(0, len(names), args.batch_cities), start=1):
        group = names[offset:offset + args.batch_cities]
        logging.info("Requesting historical data group %d/%d: %s", index, total_groups, ", ".join(group))
        total += import_group(start_date, end_date, group, index)
        time.sleep(1.0)  # Keep calls polite to the public API.
    logging.info("Finished. Total daily records imported to HDFS: %d", total)
    logging.info("Historical input path: %s", HDFS_ROOT)


if __name__ == "__main__":
    main()
