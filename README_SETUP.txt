INDIA WEATHER DATABASE - IMPLEMENTATION PACK
============================================

WHAT THIS VERSION IMPLEMENTS
1. Collection for 50 configured Indian cities, every 60 seconds.
2. Retry handling for temporary API/server errors and clearer HDFS upload errors.
3. Historical daily data import to HDFS using Open-Meteo Archive API.
4. Two Hadoop Streaming MapReduce jobs:
   - City-wise temperature summary: days, average daily mean temperature,
     lowest daily minimum and highest daily maximum.
   - Monthly rainfall summary: number of daily records, total precipitation,
     and average daily precipitation for each city/month.
5. A dashboard panel that displays MapReduce results after they are generated.

FILES
-----
main.py                         FastAPI collector and dashboard
locations.py                    Shared list of 50 city coordinates
import_historical.py            Download historical data and upload JSONL to HDFS
city_temperature_mapper.py      MapReduce mapper for temperature
city_temperature_reducer.py    MapReduce reducer for temperature
monthly_rainfall_mapper.py      MapReduce mapper for monthly rainfall
monthly_rainfall_reducer.py    MapReduce reducer for monthly rainfall
run_mapreduce.cmd               Runs both Hadoop Streaming jobs and fetches outputs

INSTALL / UPDATE
----------------
1. Back up your current main.py and data folder.
2. Copy all these .py and .cmd files into the same WeatherHadoop project folder.
3. Do not delete data/weather.db. The current dashboard uses it.
4. In the existing virtual environment, install/update dependencies:
      python -m pip install --upgrade requests fastapi uvicorn certifi
5. Verify the HDFS commands work and Hadoop services are running:
      hdfs dfs -ls /

COLLECT CURRENT WEATHER
-----------------------
Start the web application:
      python -m uvicorn main:app --host 127.0.0.1 --port 8000
Open http://127.0.0.1:8000
The collector requests the configured 50 locations every 60 seconds. Temporary
API errors are retried. Never disable TLS certificate verification to fix SSL errors.
HDFS current-observation batches are uploaded under /weather/raw/date=YYYY-MM-DD.

IMPORT HISTORICAL DATA (RUN ONCE INITIALLY)
-----------------------------------------
Historical records are daily model/reanalysis data, distinct from the collector's
current-weather snapshots. The default range downloads six complete years:
      python import_historical.py --start 2020-01-01 --end 2025-12-31
This uses the Open-Meteo Historical Weather API (archive endpoint). It makes
batched requests for 5 cities at a time, writes JSON Lines to data/historical_cache,
and uploads those files under:
      /weather/historical/raw/2020-01-01_to_2025-12-31/
This may take several minutes. Keep Hadoop HDFS running. If HTTP/SSL errors occur,
try again later after checking internet access and the local certificate store.
Open-Meteo's free service is subject to its applicable terms and non-commercial
usage requirements; check its current documentation before use or redistribution.

RUN THE MAPREDUCE JOBS
----------------------
Open a new Command Prompt, go to the project folder, activate venv if desired, and run:
      run_mapreduce.cmd
The script expects HADOOP_HOME=C:\Hadoop by default, matching the observed setup. It uses relative mapper/reducer paths and the generic -files option.
If your Hadoop installation differs, set HADOOP_HOME or edit the first lines of the
script. It expects the Hadoop Streaming JAR at:
      %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-*.jar
It removes ONLY old output directories /weather/analytics/city_temperature and
/weather/analytics/monthly_rainfall before rerunning; it does not remove raw data.
The Python executable must be on PATH for Hadoop Streaming child processes.

The jobs read only /weather/historical/raw so monthly rainfall is computed from daily
precipitation sums, not from repeated minute-by-minute current snapshots. This avoids
incorrectly summing the same current precipitation value many times.

VIEW ANALYTICS IN THE DASHBOARD
-------------------------------
After run_mapreduce.cmd succeeds, keep or restart main.py, open/refresh:
      http://127.0.0.1:8000
In the "Hadoop MapReduce Analytics" section, click "Refresh analytics results".
The batch script exports results to:
      data/analytics_city_temperature.tsv
      data/analytics_monthly_rainfall.tsv
The API endpoint /api/analytics reads these files for the dashboard.

IMPORTANT NOTES
---------------
- Weather source time and collection time are different values.
- A 60-second API poll does not guarantee new model data each minute.
- API failure can interrupt collection; the retry mechanism does not bypass SSL.
- Keep pending SQLite records until HDFS confirms upload success.
- Historical data represents model/reanalysis estimates, not guaranteed station readings.
- This package deliberately does NOT include anomaly detection yet.
- The local Hadoop setup is a single-node learning prototype, not production cluster capacity.

Official references:
Open-Meteo historical API: https://open-meteo.com/en/docs/historical-weather-api
Hadoop Streaming: https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html


TROUBLESHOOTING FOR THE WINDOWS SETUP
------------------------------------
- Extract ALL files from this ZIP into the WeatherHadoop project folder. The four mapper/reducer .py files must be beside run_mapreduce.cmd.
- The historical importer writes to /weather/historical/raw/2020-01-01_to_2025-12-31 (plain path, no '=' characters).
- If earlier failed runs created /weather/historical/raw/range, it may remain empty; it does not affect the new import.
- After a successful import, verify with: hdfs dfs -ls -R /weather/historical/raw
- Then run run_mapreduce.cmd. It checks that JSONL input files and mapper/reducer programs exist before submitting jobs.
