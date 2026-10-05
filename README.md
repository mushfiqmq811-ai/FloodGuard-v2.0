# FloodGuard BD — GloFAS-Only Production Build

FloodGuard BD keeps the existing visual language and uses **Copernicus CEMS / GloFAS** as its only operational hydrological source. BWDB scraping, synthetic hydrological fallback and fake observed water levels are not part of the production data path.

## Operational data
- Dataset: `cems-glofas-forecast`
- Variable: `river_discharge_in_the_last_24_hours`
- Unit: m³/s
- Operational model: LISFLOOD
- FloodGuard display horizon: 15 days
- Ensemble evidence: P10 / P50 / P90 when returned by EWDS
- Automatic issue-date fallback: newest available daily issue, stepping back several days when publication lags
- Persistent real snapshot: the last successfully fetched authentic dataset is retained locally for process restarts

## General Mode
Public-facing monitoring includes:
- Home / current situation
- 20 strategic GloFAS forecast zones
- Bangladesh map
- 15-day forecast cards, table and charts
- Forecast-window signal ranking and national distribution
- Weather, earthquake and cyclone hazard feeds
- GloFAS scenario lab
- Grounded FloodGuard Copilot
- Account, saved zone and in-app alert history
- Email and browser push notification channels when their server credentials are configured

## Research Mode
Research is a separate technical page rather than a second fake data pipeline. It exposes:
- Source provenance
- GloFAS/LISFLOOD architecture
- Ensemble P10/P50/P90 evidence
- Operational forecast replay by issue date
- Forecast-vs-GloFAS-historical verification with MAE, RMSE, bias and correlation
- Explicit scientific limitation: the verification target is a GloFAS modelled historical product, not independent Bangladesh gauge truth

## Signal definition
FloodGuard does **not** invent a Bangladesh danger-stage threshold from GloFAS discharge. The public signal is a transparent percentile score within each zone's loaded 15-day forecast window. It is experimental decision support, not an official flood warning.

## Deployment
See `DEPLOY_RENDER.txt` for the exact Render configuration. The required hydrological secret is `CDS_API_KEY`. Gemini is optional because FloodGuard includes a grounded local explainer fallback; SMTP and VAPID are optional notification channels.
