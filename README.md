# FloodGuard BD — GloFAS-only production build

FloodGuard BD keeps the existing UI and uses **Copernicus CEMS / GloFAS** as its only operational hydrological source. BWDB scraping, synthetic hydrological fallback and fake observed water levels are not part of the production data path.

## Operational data
- Dataset: `cems-glofas-forecast`
- Variable: `river_discharge_in_the_last_24_hours`
- Unit: m³/s
- Operational model: LISFLOOD / GloFAS v4.0 operational forecast
- Forecast horizon: 15 days
- Monitoring coverage: 6 selected Bangladesh target regions
- Target coordinates are locations; the backend resolves the nearest finite GloFAS river-discharge grid cell in the returned data. They are **not Bangladesh gauge stations**.
- Ensemble evidence: P10 / P50 / P90 is fetched on demand for the selected Research zone.
- Issue-date fallback: newest available daily issue, then up to three prior UTC dates when publication lags.

## General Mode
The existing public UI remains intact and includes current situation, selected-zone forecast, Bangladesh map, 15-day outlook, analytics, hazards, simulation, alerts and Copilot.

## Research Mode
Research exposes provenance, ensemble P10/P50/P90 evidence, operational forecast replay by issue date, and forecast-vs-historical GloFAS verification. Verification is explicitly **model-vs-model**, not independent gauge validation.

## Signal definition
FloodGuard does not invent a Bangladesh danger-stage threshold from discharge. Its public high-flow signal is a transparent percentile position inside the selected zone's loaded 15-day GloFAS forecast window. It is not a flood probability and not an official Bangladesh warning.

## Render Free architecture
Render Free provides 0.1 CPU and 512 MB RAM, spins down after inactivity, loses local filesystem state on restart/spin-down, and blocks outbound SMTP ports 25/465/587. citeturn0search0turn0search1

Therefore this build uses one Gunicorn worker/thread, demand-driven GloFAS refresh, compact geographic coverage, no startup download, best-effort local cache, and HTTP email webhook delivery through `EMAIL_SCRIPT_URL`. Background alert looping is off by default; a successful GloFAS refresh can dispatch configured alerts while the service is awake.

Required Render secret: `CDS_API_KEY`. Gemini is optional. Browser push requires VAPID credentials.
