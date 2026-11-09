
@echo off
setlocal

echo ============================================================
echo India Weather Database - Hadoop MapReduce
echo ============================================================

set "INPUT=/weather/historical/raw/2020-01-01_to_2025-12-31"
set "STREAMING_JAR=C:\Hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar"
set "BASE_DIR=%~dp0"
set "OUTPUT_TEMP=/weather/output/city_temperature"
set "OUTPUT_RAIN=/weather/output/monthly_rainfall"

if not exist "%STREAMING_JAR%" (
    echo ERROR: Hadoop Streaming JAR not found.
    goto :error
)

echo.
echo Checking historical input files...
hdfs dfs -ls "%INPUT%"
if errorlevel 1 goto :error

echo.
echo [1/2] Running city-wise temperature MapReduce job...

hdfs dfs -test -e "%OUTPUT_TEMP%"
if not errorlevel 1 hdfs dfs -rm -r -skipTrash "%OUTPUT_TEMP%"

hadoop jar "%STREAMING_JAR%" ^
  -files "%BASE_DIR%city_temperature_mapper.py,%BASE_DIR%city_temperature_reducer.py" ^
  -input "%INPUT%/*.jsonl" ^
  -output "%OUTPUT_TEMP%" ^
  -mapper "python city_temperature_mapper.py" ^
  -reducer "python city_temperature_reducer.py"

if errorlevel 1 goto :error

echo.
echo [2/2] Running monthly rainfall MapReduce job...

hdfs dfs -test -e "%OUTPUT_RAIN%"
if not errorlevel 1 hdfs dfs -rm -r -skipTrash "%OUTPUT_RAIN%"

hadoop jar "%STREAMING_JAR%" ^
  -files "%BASE_DIR%monthly_rainfall_mapper.py,%BASE_DIR%monthly_rainfall_reducer.py" ^
  -input "%INPUT%/*.jsonl" ^
  -output "%OUTPUT_RAIN%" ^
  -mapper "python monthly_rainfall_mapper.py" ^
  -reducer "python monthly_rainfall_reducer.py"

if errorlevel 1 goto :error

echo.
echo ============================================================
echo MapReduce jobs completed successfully.
echo ============================================================

echo.
echo City-wise temperature results:
hdfs dfs -cat "%OUTPUT_TEMP%/part-*"

echo.
echo Monthly rainfall results:
hdfs dfs -cat "%OUTPUT_RAIN%/part-*"

echo.
echo Open the dashboard and click Refresh analytics results.
goto :end

:error
echo.
echo ERROR: A command failed. Read the error above.
echo Historical input files have not been deleted.

:end
echo.
pause
