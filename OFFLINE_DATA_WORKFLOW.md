# FloodGuard BD — Offline data workflow

1. Download GloFAS historical data manually/API-assisted from Copernicus EWDS.
2. Give the downloaded source files to Gemini using `GEMINI_DATA_PROCESSING_PROMPT.md`.
3. Gemini creates `data/glofas_processed.csv`, `data/DATA_QUALITY_REPORT.md`, and `data/glofas_manifest.json`.
4. Put the processed CSV into this repository at `data/glofas_processed.csv`.
5. Install training dependencies: `pip install -r requirements-train.txt`.
6. Train the local model: `python scripts/train_model.py`.
7. This creates `models/floodguard_model.joblib`.
8. Run locally: `python app.py` or `gunicorn --workers 1 --threads 1 --timeout 120 --bind 0.0.0.0:$PORT app:app`.
9. The website never calls GloFAS at runtime. It reads the local processed archive.
10. Later, process FFWC PDFs as a separate observation/validation dataset; do not overwrite GloFAS discharge values.

The six-zone CSV is deliberately much smaller than the raw Bangladesh-wide grid archive while retaining the complete daily history for the project zones.
