# FloodGuard BD — Offline architecture

Downloaded authoritative GloFAS historical files are processed offline into a compact six-zone daily dataset. The website never calls GloFAS/Copernicus at runtime.

Authoritative source
  -> GloFAS historical v5.0 / LISFLOOD
  -> offline extraction and QA
  -> data/glofas_processed.csv
  -> optional offline Random Forest training
  -> models/floodguard_model.joblib
  -> Flask API
  -> existing FloodGuard UI

Later:
  FFWC PDF/observed data
  -> separate observation/validation dataset
  -> validation + calibration evidence

The public UI remains unchanged in layout/style. Only data-source semantics and backend behavior are updated.
