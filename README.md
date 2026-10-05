# FloodGuard BD — GloFAS-Only Production Build

This build keeps the existing FloodGuard BD visual language while switching the operational data path to one authentic source: Copernicus CEMS / GloFAS. BWDB scraping, synthetic fallback data and fake observed water levels are removed from the production path.

## Data architecture
- **Live/operational source:** Copernicus CEMS / GloFAS via `https://ewds.climate.copernicus.eu/api`
- **Dataset:** `cems-glofas-forecast`
- **Variable:** `river_discharge_in_the_last_24_hours`
- **Unit:** m³/s
- **Horizon:** 15 days shown in FloodGuard; the source request retrieves 30 days.
- **Ensemble:** perturbed forecasts are requested so P10/P50/P90 information can be exposed when returned by EWDS.
- **Historical replay:** the Research endpoint can request the actual operational GloFAS forecast issued on a supplied historical date.

## Risk definition
FloodGuard does not invent a national danger-level threshold. The current release derives a **relative high-flow signal** from each zone's 15-day GloFAS forecast window. This is transparent experimental decision support, not an official flood warning.

## Research mode
The dashboard includes a Research page with source provenance, live-zone coverage, scientific status, ensemble evidence and a per-zone GloFAS forecast table.

## Deployment
Set the environment variables in `DEPLOY_RENDER.txt`, then deploy with:

```bash
pip install -r requirements.txt
gunicorn --bind 0.0.0.0:$PORT app:app
```

No training command is required at Render build time.
