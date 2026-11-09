import asyncio
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "weather.db"

# Open-Meteo does not require an API key for its free,
# non-commercial service. This URL is used by the code below.
API_URL = "https://api.open-meteo.com/v1/forecast"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"

# Collection every 60 seconds; archive accumulated records to
# HDFS approximately every 15 minutes.
COLLECTION_SECONDS = 60
ARCHIVE_SECONDS = 15 * 60
TIMEZONE = ZoneInfo("Asia/Kolkata")
HDFS_ROOT = "/weather/raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# The same 50-city list is shared by the collector and historical importer.
from locations import LOCATIONS


# ============================================================
# SQLITE DATABASE
# ============================================================

def connect_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db():
    with connect_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                weather_time TEXT,
                collected_at TEXT NOT NULL,
                temperature_c REAL,
                humidity_pct REAL,
                precipitation_mm REAL,
                wind_speed_kmh REAL,
                pressure_hpa REAL,
                cloud_cover_pct REAL,
                weather_code INTEGER,
                archived INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_weather_city_id
            ON observations(city, id)
        """)
        conn.commit()


# ============================================================
# HTTP RETRIES
# ============================================================

def request_with_retries(url, *, params=None, timeout=30, attempts=3):
    """Retry temporary network/server errors; never disable TLS verification."""
    last_error = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            # Explicitly retry common temporary API/gateway failures.
            if response.status_code in (429, 500, 502, 503, 504) and attempt < attempts - 1:
                wait = 2 ** attempt
                logging.warning("HTTP %s from %s; retrying in %s second(s)", response.status_code, url, wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            wait = 2 ** attempt
            logging.warning("Request failed (%s); retrying in %s second(s)", exc, wait)
            time.sleep(wait)
    raise last_error


# ============================================================
# WEATHER API COLLECTION
# ============================================================

def collect_weather():
    """Fetch one batched Open-Meteo response for all configured cities."""
    names = list(LOCATIONS.keys())

    params = {
        "latitude": ",".join(str(LOCATIONS[name][0]) for name in names),
        "longitude": ",".join(str(LOCATIONS[name][1]) for name in names),
        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "wind_speed_10m,"
            "pressure_msl,"
            "cloud_cover,"
            "weather_code"
        ),
        "timezone": "Asia/Kolkata",
    }

    response = request_with_retries(API_URL, params=params, timeout=45)
    response.raise_for_status()
    payload = response.json()

    # Open-Meteo returns a list when multiple locations are requested.
    results = payload if isinstance(payload, list) else [payload]

    if len(results) != len(names):
        raise ValueError(
            f"Expected {len(names)} location results, received {len(results)}."
        )

    collected_at = datetime.now(TIMEZONE).isoformat()
    records = []

    for name, result in zip(names, results):
        current = result.get("current")
        if not isinstance(current, dict):
            logging.warning("No current weather returned for %s", name)
            continue

        latitude, longitude = LOCATIONS[name]

        records.append((
            name,
            latitude,
            longitude,
            current.get("time"),
            collected_at,
            current.get("temperature_2m"),
            current.get("relative_humidity_2m"),
            current.get("precipitation"),
            current.get("wind_speed_10m"),
            current.get("pressure_msl"),
            current.get("cloud_cover"),
            current.get("weather_code"),
        ))

    if not records:
        raise ValueError("The API returned no usable weather records.")

    with connect_db() as conn:
        conn.executemany("""
            INSERT INTO observations (
                city, latitude, longitude, weather_time, collected_at,
                temperature_c, humidity_pct, precipitation_mm,
                wind_speed_kmh, pressure_hpa, cloud_cover_pct, weather_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        conn.commit()

    logging.info(
        "Collected %d of %d configured locations.",
        len(records), len(LOCATIONS)
    )


# ============================================================
# HDFS COMMAND HELPER (Windows-compatible)
# ============================================================

def find_hdfs_cmd() -> Path:
    """Find Hadoop's Windows hdfs.cmd launcher."""
    candidates = []

    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        candidates.append(Path(hadoop_home) / "bin" / "hdfs.cmd")
        candidates.append(Path(hadoop_home) / "bin" / "hdfs.exe")

    for command_name in ("hdfs.cmd", "hdfs.bat", "hdfs.exe"):
        found = shutil.which(command_name)
        if found:
            candidates.append(Path(found))

    # Fallback for the Hadoop installation path used on this machine.
    candidates.append(Path(r"C:\Hadoop\bin\hdfs.cmd"))

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        "Cannot locate hdfs.cmd. Check HADOOP_HOME or update the fallback path "
        "in main.py to your Hadoop installation's bin/hdfs.cmd file."
    )


def run_hdfs(args, timeout):
    """Run hdfs.cmd through Windows cmd.exe and retain Hadoop's error text."""
    hdfs_cmd = find_hdfs_cmd()
    arguments = subprocess.list2cmdline([str(value) for value in args])
    command = f'call "{hdfs_cmd}" {arguments}'
    result = subprocess.run(
        command,
        shell=True,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"HDFS command failed (exit {result.returncode}): {command}\n"
            f"STDOUT: {result.stdout.strip()}\nSTDERR: {result.stderr.strip()}"
        )
    if result.stdout.strip():
        logging.info("HDFS: %s", result.stdout.strip())
    return result


# ============================================================
# HDFS ARCHIVING
# ============================================================

def archive_to_hdfs():
    """Upload unarchived SQLite records as one JSON Lines batch to HDFS."""
    with connect_db() as conn:
        rows = conn.execute("""
            SELECT * FROM observations
            WHERE archived = 0
            ORDER BY id
        """).fetchall()

    if not rows:
        logging.info("No new records to archive to HDFS.")
        return

    now = datetime.now(TIMEZONE)
    batch_name = now.strftime("weather_batch_%Y%m%d_%H%M%S.jsonl")
    local_file = DATA_DIR / batch_name
    hdfs_dir = f"{HDFS_ROOT}/{now:%Y-%m-%d}"
    destination = f"{hdfs_dir}/{batch_name}"

    with local_file.open("w", encoding="utf-8") as file:
        for row in rows:
            record = dict(row)
            record.pop("archived", None)
            file.write(json.dumps(record, ensure_ascii=False) + "\n")

    try:
        run_hdfs(["dfs", "-mkdir", "-p", hdfs_dir], timeout=60)
        run_hdfs(
            ["dfs", "-put", "-f", str(local_file), destination],
            timeout=120,
        )

        ids = [row["id"] for row in rows]
        placeholders = ",".join("?" for _ in ids)

        with connect_db() as conn:
            conn.execute(
                f"UPDATE observations SET archived = 1 "
                f"WHERE id IN ({placeholders})",
                ids,
            )
            conn.commit()

        local_file.unlink(missing_ok=True)
        logging.info("Archived %d records to HDFS: %s", len(rows), destination)

    except Exception:
        # Keep SQLite records marked unarchived so a later attempt can retry.
        logging.exception("HDFS upload failed. Records remain pending.")
        raise


# ============================================================
# BACKGROUND COLLECTION
# ============================================================

async def collector_loop():
    # Archive pending rows during the first collection cycle, then every 15 minutes.
    last_archive = time.monotonic() - ARCHIVE_SECONDS

    while True:
        cycle_started = time.monotonic()

        try:
            await asyncio.to_thread(collect_weather)
        except Exception:
            logging.exception("Weather collection failed; will retry.")

        if time.monotonic() - last_archive >= ARCHIVE_SECONDS:
            try:
                await asyncio.to_thread(archive_to_hdfs)
                last_archive = time.monotonic()
            except Exception:
                # Leave the timer due so the next cycle retries.
                pass

        elapsed = time.monotonic() - cycle_started
        await asyncio.sleep(max(1, COLLECTION_SECONDS - elapsed))


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_db()
    task = asyncio.create_task(collector_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="India Weather Pattern Analysis",
    description="Weather collection, SQLite latest-value API, and HDFS archive.",
    lifespan=lifespan,
)


# ============================================================
# JSON ENDPOINT FOR DASHBOARD
# ============================================================

@app.get("/api/latest")
def latest_weather():
    with connect_db() as conn:
        rows = conn.execute("""
            SELECT o.*
            FROM observations AS o
            JOIN (
                SELECT city, MAX(id) AS latest_id
                FROM observations
                GROUP BY city
            ) AS latest
            ON o.id = latest.latest_id
            ORDER BY o.city
        """).fetchall()

        total_records = conn.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0]

        pending_records = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE archived = 0"
        ).fetchone()[0]

    return {
        "primary_location": "Belagavi",
        "locations_configured": len(LOCATIONS),
        "locations_reporting": len(rows),
        "total_records": total_records,
        "pending_hdfs_records": pending_records,
        "refreshed_at": datetime.now(TIMEZONE).isoformat(),
        "locations": [dict(row) for row in rows],
    }


# ============================================================
# SEARCH CITIES WORLDWIDE (NOT LIMITED TO LOCATIONS IN LOCATIONS)
# ============================================================

@app.get("/api/city-search")
def city_search(q: str = ""):
    """Search Open-Meteo's global geocoding index for a city or place name."""
    query = q.strip()
    if len(query) < 2:
        return {"query": query, "results": []}
    if len(query) > 100:
        raise HTTPException(status_code=400, detail="Search text must be 100 characters or fewer.")

    try:
        response = request_with_retries(
            GEOCODING_URL,
            params={"name": query, "count": 10, "language": "en", "format": "json"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logging.exception("Global city search failed")
        raise HTTPException(
            status_code=502,
            detail="City search could not reach Open-Meteo. Check your internet connection and try again.",
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="City search returned invalid JSON.") from exc

    tracked_coordinates = list(LOCATIONS.values())
    results = []
    for item in payload.get("results", []) or []:
        try:
            latitude = float(item["latitude"])
            longitude = float(item["longitude"])
        except (KeyError, TypeError, ValueError):
            continue

        # Mark a result as tracked if its coordinates correspond to a configured city.
        is_tracked = any(
            abs(latitude - tracked_lat) < 0.02 and abs(longitude - tracked_lon) < 0.02
            for tracked_lat, tracked_lon in tracked_coordinates
        )
        results.append({
            "id": item.get("id"),
            "name": item.get("name", "Unknown location"),
            "country": item.get("country", ""),
            "country_code": item.get("country_code", ""),
            "admin1": item.get("admin1", ""),
            "latitude": latitude,
            "longitude": longitude,
            "timezone": item.get("timezone", ""),
            "population": item.get("population"),
            "is_tracked": is_tracked,
        })

    return {"query": query, "results": results}


@app.get("/api/city-weather")
def city_weather(
    latitude: float,
    longitude: float,
    name: str = "Selected city",
    country: str = "",
):
    """Fetch weather on demand for any coordinates returned by city search.

    This endpoint does not add the searched location to the scheduled collection,
    SQLite observations table, or HDFS archive.
    """
    if not -90 <= latitude <= 90:
        raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90.")
    if not -180 <= longitude <= 180:
        raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180.")

    try:
        response = request_with_retries(
            API_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current": (
                    "temperature_2m,relative_humidity_2m,precipitation,"
                    "wind_speed_10m,pressure_msl,cloud_cover,weather_code"
                ),
                "timezone": "auto",
            },
            timeout=25,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logging.exception("On-demand weather fetch failed for %s", name)
        raise HTTPException(
            status_code=502,
            detail="Could not fetch weather for this location. Check your connection and try again.",
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Weather service returned invalid JSON.") from exc

    current = payload.get("current")
    if not isinstance(current, dict):
        raise HTTPException(status_code=502, detail="No current weather data was returned for this location.")

    return {
        "name": name[:100],
        "country": country[:100],
        "latitude": payload.get("latitude", latitude),
        "longitude": payload.get("longitude", longitude),
        "timezone": payload.get("timezone", ""),
        "weather_time": current.get("time"),
        "fetched_at": datetime.now(TIMEZONE).isoformat(),
        "temperature_c": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "pressure_hpa": current.get("pressure_msl"),
        "cloud_cover_pct": current.get("cloud_cover"),
        "weather_code": current.get("weather_code"),
        "stored_in_scheduled_database": False,
    }


# ============================================================
# BROWSER DASHBOARD
# ============================================================

DASHBOARD = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>India Weather Pattern Analysis</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
:root { font-family: Arial, sans-serif; color: #182235; background: #f3f6fb; }
* { box-sizing: border-box; }
body { margin: 0; }
header { background: #163c68; color: white; padding: 22px 5%; }
header h1 { margin: 0 0 8px; font-size: 25px; }
header p { margin: 0; opacity: .88; }
main { max-width: 1500px; margin: auto; padding: 22px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 14px; }
.card, .panel { background: white; padding: 18px; border-radius: 12px; box-shadow: 0 2px 9px #152d4810; }
.label { color: #667085; font-size: 13px; }
.value { font-size: 26px; font-weight: bold; margin-top: 9px; overflow-wrap: anywhere; }
.layout { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-top: 18px; }
.table-wrap { overflow-x: auto; max-height: 620px; }
.search-row { display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }
.search-row input { flex:1; min-width:220px; padding:12px 13px; border:1px solid #cbd5e1; border-radius:8px; font-size:15px; }
.search-row button { padding:11px 16px; border:0; border-radius:8px; background:#1e5a91; color:white; font-weight:bold; cursor:pointer; }
.search-row button.secondary { background:#e8eef6; color:#1c3553; }
.search-hint, #citySearchStatus { color:#667085; font-size:13px; margin-top:10px; }
.search-results { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; margin-top:12px; }
.city-result { display:block; text-align:left; padding:12px; border:1px solid #d9e3ef; border-radius:9px; background:#f8fbff; cursor:pointer; color:#182235; }
.city-result:hover, .city-result:focus { border-color:#1e5a91; background:#eef6ff; }
.city-result strong { display:block; margin-bottom:5px; }
.city-result small { display:block; line-height:1.4; }
.search-weather { margin-top:14px; border:1px solid #d9e3ef; border-radius:10px; padding:16px; background:#f8fbff; }
.search-weather h4 { margin:0 0 5px; font-size:18px; }
.search-metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px; margin-top:12px; }
.search-metric { background:white; border-radius:8px; padding:12px; }
.search-metric .metric-value { font-size:20px; font-weight:bold; margin-top:5px; }
.table-filter-row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin:10px 0 14px; }
.table-filter-row input { padding:9px 11px; border:1px solid #cbd5e1; border-radius:7px; min-width:220px; }








table { border-collapse: collapse; width: 100%; background: white; font-size: 13px; }
th, td { text-align: left; padding: 12px 10px; border-bottom: 1px solid #e8edf4; white-space: nowrap; }
th { background: #edf3fa; position: sticky; top: 0; }
.status { display: inline-block; border-radius: 30px; padding: 5px 9px; background: #d9f5e5; color: #17683b; font-size: 12px; }
small { color: #667085; }
@media(max-width:800px) { .layout { grid-template-columns: 1fr; } main { padding: 12px; } }
</style>
</head>
<body>
<header>
  <h1>🌦 India Weather Pattern Analysis</h1>
  <p>Primary location: Belagavi · 50 cities across India · 60-second collection cycle</p>
</header>
<main>
  <div class="cards">
    <div class="card"><div class="label">Locations reporting</div><div class="value" id="count">--</div></div>
    <div class="card"><div class="label">Average temperature</div><div class="value" id="avg">--</div></div>
    <div class="card"><div class="label">Highest temperature</div><div class="value" id="high">--</div></div>
    <div class="card"><div class="label">Total collected records</div><div class="value" id="total">--</div></div>
  </div>

  <section class="panel" style="margin-top:18px">
    <h3>🔎 Search a city</h3>
    <p>Filter your tracked cities or search for a city anywhere in the world. Select a result to fetch its latest available weather.</p>
    <div class="search-row">
      <input id="citySearchInput" type="search" placeholder="Enter a city, e.g. Belagavi, Jaipur, London, Tokyo" autocomplete="off" maxlength="100">
      <button id="searchCityButton" type="button">Search worldwide</button>
      <button id="clearCitySearchButton" class="secondary" type="button">Clear</button>
    </div>
    <div id="localFilterStatus" class="search-hint">The table below shows all tracked cities. Typing here also filters that table.</div>
    <div id="citySearchStatus" role="status" aria-live="polite"></div>
    <div id="citySearchResults" class="search-results"></div>
    <section id="searchWeatherCard" class="search-weather" hidden aria-live="polite"></section>
  </section>

  <div class="layout">
    <section class="panel">
      <h3>Temperature by location</h3>
      <canvas id="chart"></canvas>
    </section>
    <section class="panel">
      <h3>Collection status</h3>
      <p><span class="status" id="status">Waiting for data...</span></p>
      <p class="label">Dashboard refresh time</p><p id="refresh">--</p>
      <p class="label">Weather source timestamp (Belagavi)</p><p id="weatherTime">--</p>
      <p class="label">Records waiting for HDFS archive</p><div class="value" id="pending">--</div>
      <small>HDFS batches are attempted approximately every 15 minutes.</small>
    </section>
  </div>

  <section class="panel" style="margin-top:18px">
    <h3>Latest weather across India</h3>
    <div class="table-filter-row">
      <input id="trackedTableFilter" type="search" placeholder="Filter tracked city table..." autocomplete="off">
      <small id="trackedTableCount">Loading tracked cities...</small>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Location</th><th>Temperature °C</th><th>Humidity %</th>
          <th>Precipitation mm</th><th>Wind km/h</th><th>Pressure hPa</th>
          <th>Cloud cover %</th><th>Weather source time</th><th>Collected at</th>
        </tr></thead>
        <tbody id="table"></tbody>
      </table>
    </div>
  </section>
  <section class="panel" style="margin-top:18px">
    <h3>🌤 Weather Insights</h3>
    <p class="label">Calculated from the latest available readings for tracked cities.</p>
    <div class="cards" style="margin-top:12px">
      <div class="card"><div class="label">Lowest temperature</div><div class="value" id="insightLow">--</div></div>
      <div class="card"><div class="label">Highest temperature</div><div class="value" id="insightHigh">--</div></div>
      <div class="card"><div class="label">Average humidity</div><div class="value" id="insightHumidity">--</div></div>
      <div class="card"><div class="label">Cities reporting precipitation</div><div class="value" id="insightRain">--</div></div>
    </div>
    <p id="insightSummary" class="label" style="margin-top:12px">Waiting for weather readings...</p>
  </section>
  <p><small>Source: Open-Meteo. Current values are weather-model data, not guaranteed live station measurements. The source timestamp may not change every minute.</small></p>
</main>

<script>
let chart;
let latestRows = [];

function fmt(v, digits=1) {
  return v == null ? "—" : Number(v).toFixed(digits);
}

function safeText(value) {
  return value == null || value === "" ? "—" : String(value);
}

function renderTrackedTable() {
  const filterValue = document.getElementById("trackedTableFilter").value.trim().toLocaleLowerCase();
  const shownRows = latestRows.filter(x => x.city.toLocaleLowerCase().includes(filterValue));
  const tbody = document.getElementById("table");
  tbody.replaceChildren();

  for (const x of shownRows) {
    const tr = document.createElement("tr");
    const values = [
      x.city, fmt(x.temperature_c), fmt(x.humidity_pct, 0),
      fmt(x.precipitation_mm), fmt(x.wind_speed_kmh),
      fmt(x.pressure_hpa), fmt(x.cloud_cover_pct, 0),
      safeText(x.weather_time),
      x.collected_at ? new Date(x.collected_at).toLocaleString() : "—"
    ];
    for (const value of values) {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }

  document.getElementById("trackedTableCount").textContent =
    `Showing ${shownRows.length} of ${latestRows.length} tracked cities`;
  const hint = document.getElementById("localFilterStatus");
  if (filterValue && shownRows.length === 0) {
    hint.textContent = `No tracked city matches “${filterValue}”. Use Search worldwide to look beyond the tracked list.`;
  } else if (filterValue) {
    hint.textContent = `Found ${shownRows.length} matching tracked city/cities. Use Search worldwide to search beyond the tracked list too.`;
  } else {
    hint.textContent = "The table below shows all tracked cities. Typing here also filters that table.";
  }
}

async function refreshDashboard() {
  try {
    const response = await fetch("/api/latest", {cache:"no-store"});
    if (!response.ok) throw new Error("Dashboard API unavailable");
    const data = await response.json();
    const rows = data.locations;
    latestRows = rows;

    document.getElementById("count").textContent =
      `${data.locations_reporting}/${data.locations_configured}`;
    document.getElementById("total").textContent =
      data.total_records.toLocaleString();
    document.getElementById("pending").textContent =
      data.pending_hdfs_records.toLocaleString();
    document.getElementById("refresh").textContent =
      new Date(data.refreshed_at).toLocaleString();
    document.getElementById("status").textContent =
      rows.length ? "Dashboard connected" : "Waiting for first collection";

    const temps = rows.filter(x => x.temperature_c != null);
    const average = temps.length
      ? temps.reduce((sum, x) => sum + x.temperature_c, 0) / temps.length
      : null;
    const hottest = temps.length
      ? temps.reduce((a, b) => a.temperature_c > b.temperature_c ? a : b)
      : null;

    document.getElementById("avg").textContent =
      average == null ? "—" : fmt(average) + " °C";
    document.getElementById("high").textContent =
      hottest ? `${hottest.city}: ${fmt(hottest.temperature_c)} °C` : "—";

    const belagavi = rows.find(x => x.city === "Belagavi");
    document.getElementById("weatherTime").textContent =
      belagavi?.weather_time || "—";

    renderTrackedTable();

    const validTemps = rows.filter(x => x.temperature_c != null && Number.isFinite(Number(x.temperature_c)));
    const validHumidity = rows.filter(x => x.humidity_pct != null && Number.isFinite(Number(x.humidity_pct)));
    const wetCities = rows.filter(x => Number(x.precipitation_mm) > 0);
    const lowest = validTemps.length ? validTemps.reduce((a, b) => Number(a.temperature_c) < Number(b.temperature_c) ? a : b) : null;
    const highest = validTemps.length ? validTemps.reduce((a, b) => Number(a.temperature_c) > Number(b.temperature_c) ? a : b) : null;
    const averageHumidity = validHumidity.length ? validHumidity.reduce((sum, x) => sum + Number(x.humidity_pct), 0) / validHumidity.length : null;

    document.getElementById("insightLow").textContent = lowest ? `${lowest.city}: ${fmt(lowest.temperature_c)} °C` : "—";
    document.getElementById("insightHigh").textContent = highest ? `${highest.city}: ${fmt(highest.temperature_c)} °C` : "—";
    document.getElementById("insightHumidity").textContent = averageHumidity == null ? "—" : `${fmt(averageHumidity, 0)}%`;
    document.getElementById("insightRain").textContent = `${wetCities.length} / ${rows.length}`;
    document.getElementById("insightSummary").textContent = validTemps.length
      ? `Based on ${validTemps.length} cities with temperature readings. Precipitation is the current reported amount, not a monthly rainfall total.`
      : "No usable weather readings are available yet.";

    if (typeof Chart !== "undefined") {
      const labels = temps.map(x => x.city);
      const values = temps.map(x => x.temperature_c);
      if (chart) {
        chart.data.labels = labels;
        chart.data.datasets[0].data = values;
        chart.update();
      } else {
        chart = new Chart(document.getElementById("chart"), {
          type: "bar",
          data: { labels, datasets: [{ label: "Temperature °C", data: values }] },
          options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { x: { ticks: { autoSkip: false, maxRotation: 70, minRotation: 45 } } }
          }
        });
      }
    }
  } catch (error) {
    document.getElementById("status").textContent = "Connection error — retrying";
    console.error(error);
  }
}

async function searchWorldwide() {
  const input = document.getElementById("citySearchInput");
  const query = input.value.trim();
  const status = document.getElementById("citySearchStatus");
  const resultsBox = document.getElementById("citySearchResults");
  const weatherCard = document.getElementById("searchWeatherCard");
  resultsBox.replaceChildren();
  weatherCard.hidden = true;

  if (query.length < 2) {
    status.textContent = "Enter at least two characters to search for a city.";
    return;
  }

  status.textContent = "Searching city names worldwide...";
  const button = document.getElementById("searchCityButton");
  button.disabled = true;
  try {
    const response = await fetch(`/api/city-search?q=${encodeURIComponent(query)}`, {cache:"no-store"});
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || "City search failed.");

    if (!payload.results || payload.results.length === 0) {
      status.textContent = `No locations found for “${query}”. Try another spelling or add a country, e.g. “Springfield, United States”.`;
      return;
    }

    status.textContent = `Found ${payload.results.length} result(s). Select one to fetch its weather.`;
    for (const place of payload.results) {
      const resultButton = document.createElement("button");
      resultButton.type = "button";
      resultButton.className = "city-result";

      const title = document.createElement("strong");
      title.textContent = [place.name, place.admin1, place.country].filter(Boolean).join(", ");
      const detail = document.createElement("small");
      const trackingLabel = place.is_tracked ? "Already in scheduled collection" : "On-demand city (not in scheduled collection)";
      detail.textContent = `${trackingLabel} · ${Number(place.latitude).toFixed(4)}, ${Number(place.longitude).toFixed(4)}`;
      resultButton.append(title, detail);
      resultButton.addEventListener("click", () => loadSearchedWeather(place));
      resultsBox.appendChild(resultButton);
    }
  } catch (error) {
    status.textContent = error.message || "City search failed. Check the connection and try again.";
  } finally {
    button.disabled = false;
  }
}

function addMetric(container, label, value) {
  const metric = document.createElement("div");
  metric.className = "search-metric";
  const lab = document.createElement("div");
  lab.className = "label";
  lab.textContent = label;
  const val = document.createElement("div");
  val.className = "metric-value";
  val.textContent = value;
  metric.append(lab, val);
  container.appendChild(metric);
}

async function loadSearchedWeather(place) {
  const status = document.getElementById("citySearchStatus");
  const card = document.getElementById("searchWeatherCard");
  card.hidden = false;
  card.replaceChildren();
  status.textContent = `Fetching weather for ${place.name}...`;
  const heading = document.createElement("h4");
  heading.textContent = `Weather for ${[place.name, place.admin1, place.country].filter(Boolean).join(", ")}`;
  card.appendChild(heading);
  const loading = document.createElement("p");
  loading.textContent = "Loading the latest available weather data...";
  card.appendChild(loading);

  try {
    const params = new URLSearchParams({
      latitude: String(place.latitude),
      longitude: String(place.longitude),
      name: place.name || "Selected city",
      country: place.country || ""
    });
    const response = await fetch(`/api/city-weather?${params.toString()}`, {cache:"no-store"});
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || "Could not fetch weather for this city.");

    card.replaceChildren();
    card.appendChild(heading);
    const coordinates = document.createElement("p");
    coordinates.className = "label";
    coordinates.textContent = `Weather source time: ${data.weather_time || "—"} · Time zone: ${data.timezone || "—"}`;
    card.appendChild(coordinates);
    const metrics = document.createElement("div");
    metrics.className = "search-metrics";
    addMetric(metrics, "Temperature", data.temperature_c == null ? "—" : `${fmt(data.temperature_c)} °C`);
    addMetric(metrics, "Humidity", data.humidity_pct == null ? "—" : `${fmt(data.humidity_pct, 0)}%`);
    addMetric(metrics, "Precipitation", data.precipitation_mm == null ? "—" : `${fmt(data.precipitation_mm)} mm`);
    addMetric(metrics, "Wind speed", data.wind_speed_kmh == null ? "—" : `${fmt(data.wind_speed_kmh)} km/h`);
    addMetric(metrics, "Pressure", data.pressure_hpa == null ? "—" : `${fmt(data.pressure_hpa)} hPa`);
    addMetric(metrics, "Cloud cover", data.cloud_cover_pct == null ? "—" : `${fmt(data.cloud_cover_pct, 0)}%`);
    card.appendChild(metrics);

    const note = document.createElement("p");
    note.className = "label";
    note.textContent = place.is_tracked
      ? `This location matches a city in your scheduled collection. Data was fetched on demand at ${new Date(data.fetched_at).toLocaleString()}.`
      : `On-demand lookup only. This city is not automatically added to your scheduled 50-city database or HDFS archive. Fetched at ${new Date(data.fetched_at).toLocaleString()}.`;
    card.appendChild(note);
    status.textContent = "Weather loaded successfully.";
  } catch (error) {
    card.replaceChildren();
    const message = document.createElement("p");
    message.textContent = error.message || "Unable to load this location's weather.";
    card.appendChild(message);
    status.textContent = "Weather lookup failed.";
  }
}

document.getElementById("searchCityButton").addEventListener("click", searchWorldwide);
document.getElementById("citySearchInput").addEventListener("keydown", event => {
  if (event.key === "Enter") searchWorldwide();
});
document.getElementById("citySearchInput").addEventListener("input", () => {
  document.getElementById("trackedTableFilter").value = document.getElementById("citySearchInput").value;
  renderTrackedTable();
});
document.getElementById("trackedTableFilter").addEventListener("input", renderTrackedTable);
document.getElementById("clearCitySearchButton").addEventListener("click", () => {
  document.getElementById("citySearchInput").value = "";
  document.getElementById("trackedTableFilter").value = "";
  document.getElementById("citySearchStatus").textContent = "";
  document.getElementById("citySearchResults").replaceChildren();
  document.getElementById("searchWeatherCard").hidden = true;
  document.getElementById("searchWeatherCard").replaceChildren();
  renderTrackedTable();
});

refreshDashboard();
refreshAnalytics();
setInterval(refreshDashboard, 60000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
