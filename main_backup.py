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
