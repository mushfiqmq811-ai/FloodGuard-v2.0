# FloodGuard BD — Real-Data Production Build

This build keeps the existing FloodGuard BD interface and replaces the previous simulated/demo data path with an authentic-source pipeline.

## Data sources
- **BWDB Hydrology chart pages:** observed water levels are fetched from the official BWDB Hydrology website. The station availability report is used to map FloodGuard zones to published BWDB stations; chart pages are then parsed for observed timestamp/level records.
- **Copernicus GloFAS / EWDS:** operational forecast access uses the current EWDS API base URL `https://ewds.climate.copernicus.eu/api` and a server-side personal access token.
- **Model:** the production model is trained only from authentic BWDB observed water-level history. If insufficient real observations are available, training aborts; it never fabricates rows.
- **Gemini:** optional AI Copilot uses `GEMINI_API_KEY` and is instructed to ground answers only in FloodGuard source data.
- **Notifications:** SMTP email and browser Web Push are supported.

## Important behavior
There is no synthetic-data fallback in the production data path. If an authentic source cannot be fetched, the affected zone reports `DATA UNAVAILABLE` and the model does not invent a prediction.

## Local / Render
Install dependencies, configure the environment variables in `DEPLOY_RENDER.txt`, then run:

```bash
python train_real_model.py
```

The training script performs a chronological holdout evaluation and writes the real-data-trained model to `model/real_flood_model.joblib` plus metadata. Generated model artifacts are ignored by Git until explicitly added.

Run the web app with:

```bash
gunicorn --bind 0.0.0.0:$PORT app:app
```
