# FloodGuard BD — Offline GloFAS historical production build

FloodGuard BD preserves the existing UI and uses a **locally processed Copernicus CEMS / GloFAS historical archive** as its hydrological data foundation. The deployed website does not call the GloFAS API at runtime.

## Data
- Dataset: `cems-glofas-historical`
- Current source version: GloFAS v5.0
- Hydrological model: LISFLOOD
- Product: consolidated historical data
- Variable: average river discharge in the last 24 hours
- Unit: m³/s
- Temporal resolution: daily
- Runtime file: `data/glofas_processed.csv`
- Six target zones: Sylhet, Kurigram, Sirajganj, Rajshahi, Faridpur, Feni
- Target coordinates identify GloFAS model locations / nearest valid river-grid cells; they are not Bangladesh gauge stations.

## General Mode
The existing public UI remains intact and includes current situation, selected-zone forecast, Bangladesh map, 15-day outlook, analytics, hazards, simulation, alerts and Copilot.

## Research Mode
Research exposes provenance, local uncertainty evidence, historical replay and forecast-vs-historical modelled-data verification. Verification is explicitly **model-vs-model**, not independent gauge validation.

## Signal definition
FloodGuard does not invent a Bangladesh danger-stage threshold from discharge. Its high-flow signal is a transparent historical percentile position and is not a flood probability or an official Bangladesh warning.

## Offline workflow
1. Download GloFAS historical source files from Copernicus EWDS.
2. Process them with `GEMINI_DATA_PROCESSING_PROMPT.md`.
3. Put the resulting `data/glofas_processed.csv` into this repository.
4. Train the optional local Random Forest with `scripts/train_model.py`.
5. Deploy the website. No GloFAS API key is needed at runtime.

See `DATASET_SCHEMA.md`, `OFFLINE_DATA_WORKFLOW.md`, and `GEMINI_DATA_PROCESSING_PROMPT.md`.
