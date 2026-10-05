# FloodGuard BD — Free Render architecture

- Hydrology: Copernicus CEMS / GloFAS only. No synthetic fallback and no BWDB runtime path.
- Monitoring: six strategically selected target locations. The coordinates are targets; the backend resolves the nearest finite GloFAS river-discharge cell returned by the requested grid.
- Forecast: 15-day operational GloFAS control forecast.
- Research: ensemble is fetched only for the selected research zone, on demand.
- Historical verification: operational GloFAS historical V4 versus operational forecast replay; this is model-vs-model verification, not independent gauge validation.
- Render Free: no startup GloFAS download, no permanent local-storage assumption, no SMTP. Email uses EMAIL_SCRIPT_URL (HTTP webhook) when configured.
- Background alerts are OFF by default; a successful GloFAS refresh triggers alert dispatch.
