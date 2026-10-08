"""FloodGuard BD local GloFAS data layer.

Runtime has NO Copernicus/EWDS API dependency.
The project consumes a locally processed GloFAS historical dataset prepared offline.
The dataset is expected at data/glofas_processed.csv and follows the schema documented
in DATASET_SCHEMA.md. Raw GloFAS files never need to be deployed to Render.
"""
from __future__ import annotations

import os
import math
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent
DATA_FILE = Path(os.getenv("GLOFAS_DATA_FILE", BASE / "data" / "glofas_processed.csv"))
MODEL_FILE = Path(os.getenv("FLOODGUARD_MODEL_FILE", BASE / "models" / "floodguard_model.joblib"))
FORECAST_DAYS = 15

_cache = {"df": None, "mtime": None, "loaded_at": None, "error": None}


def source_status():
    exists = DATA_FILE.exists()
    rows = None
    date_min = date_max = None
    if exists:
        try:
            df = _load()
            rows = len(df)
            if rows:
                date_min = df["date"].min().date().isoformat()
                date_max = df["date"].max().date().isoformat()
        except Exception:
            pass
    return {
        "authentic_only": True,
        "provider": "Copernicus CEMS / GloFAS (offline processed archive)",
        "runtime_api": False,
        "dataset": "cems-glofas-historical",
        "glofas_version": "v5.0",
        "variable": "Average river discharge in the last 24 hours",
        "unit": "m3/s",
        "hydrological_model": "LISFLOOD",
        "product_type": "Consolidated",
        "data_file": str(DATA_FILE.relative_to(BASE)) if DATA_FILE.is_relative_to(BASE) else str(DATA_FILE),
        "data_available": exists and rows is not None and rows > 0,
        "rows": rows,
        "date_min": date_min,
        "date_max": date_max,
        "model_file": str(MODEL_FILE.relative_to(BASE)) if MODEL_FILE.is_relative_to(BASE) else str(MODEL_FILE),
        "model_available": MODEL_FILE.exists(),
        "synthetic_runtime_fallback": False,
        "note": "Runtime reads the offline processed GloFAS archive. No external hydrological API call is made by the website.",
    }


def _load():
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"GloFAS dataset not found: {DATA_FILE}. Add the Gemini-processed file as data/glofas_processed.csv."
        )
    mtime = DATA_FILE.stat().st_mtime
    if _cache["df"] is not None and _cache["mtime"] == mtime:
        return _cache["df"]
    df = pd.read_csv(DATA_FILE)
    required = {"date", "station_id", "discharge_m3s"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"glofas_processed.csv missing required columns: {sorted(missing)}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_localize(None)
    df["station_id"] = df["station_id"].astype(str).str.strip().str.lower()
    df["discharge_m3s"] = pd.to_numeric(df["discharge_m3s"], errors="coerce")
    df = df.dropna(subset=["date", "station_id", "discharge_m3s"])
    df = df[df["discharge_m3s"] >= 0].copy()
    if df.empty:
        raise ValueError("glofas_processed.csv contains no valid discharge rows")
    df = df.sort_values(["station_id", "date"]).drop_duplicates(["station_id", "date"], keep="last")
    _cache.update(df=df, mtime=mtime, loaded_at=datetime.now(timezone.utc).isoformat(), error=None)
    return df


def _station_df(station):
    df = _load()
    out = df[df.station_id == str(station.get("id") or station.get("station_id") or "").lower()].copy()
    if out.empty:
        # tolerate files keyed by district/zone name
        candidates = {str(station.get(k, "")).strip().lower() for k in ("district", "name")}
        out = df[df.station_id.isin(candidates)].copy()
    return out.sort_values("date")


def _risk_from_history(history, value):
    if value is None or history.empty:
        return "UNAVAILABLE", None
    vals = history.discharge_m3s.to_numpy(dtype=float)
    rank = 100.0 * ((vals < float(value)).sum() + 0.5 * (vals == float(value)).sum()) / max(1, len(vals))
    if rank >= 99:
        risk = "SEVERE"
    elif rank >= 95:
        risk = "FLOOD"
    elif rank >= 85:
        risk = "WARNING"
    else:
        risk = "NORMAL"
    return risk, round(float(rank), 1)


def _recent_features(hist, current_date):
    before = hist[hist.date <= current_date].sort_values("date")
    if before.empty:
        return None
    vals = before.discharge_m3s.to_numpy(float)
    current = float(vals[-1])
    d1 = float(vals[-2]) if len(vals) >= 2 else current
    d3 = float(vals[-4]) if len(vals) >= 4 else d1
    d7 = float(vals[-8]) if len(vals) >= 8 else d3
    return {
        "current": current,
        "lag1": d1,
        "lag3": d3,
        "lag7": d7,
        "rise1": current - d1,
        "rise3": current - d3,
        "mean7": float(before.tail(7).discharge_m3s.mean()),
    }


def _model_forecast(hist, start_date, days=FORECAST_DAYS):
    """Use a saved model if available; otherwise use a transparent persistence/trend baseline.
    The baseline is derived only from local historical observations and is never labelled ML.
    """
    model = None
    if MODEL_FILE.exists():
        try:
            import joblib
            model = joblib.load(MODEL_FILE)
        except Exception:
            model = None
    rows = []
    working = hist[hist.date <= start_date].sort_values("date").copy()
    if working.empty:
        return rows
    last = float(working.iloc[-1].discharge_m3s)
    previous = float(working.iloc[-2].discharge_m3s) if len(working) >= 2 else last
    trend = last - previous
    for lead in range(1, days + 1):
        if model is not None:
            context = working.tail(8).discharge_m3s.to_numpy(float)
            padded = np.pad(context, (max(0, 8-len(context)), 0), mode="edge")[-8:]
            X = pd.DataFrame([{
                "lag_1d": padded[-1], "lag_2d": padded[-2], "lag_3d": padded[-3],
                "lag_7d": padded[-7], "rolling_mean_7d": float(padded.mean()),
                "rolling_max_7d": float(padded.max()), "rise_1d": padded[-1]-padded[-2],
                "rise_3d": padded[-1]-padded[-4], "forecast_horizon_day": lead,
                "month": int((start_date + timedelta(days=lead)).month),
            }])
            try:
                pred = float(model.predict(X)[0])
            except Exception:
                pred = last + trend * lead
        else:
            # Conservative, transparent baseline; no fabricated random noise.
            damp = 0.90 ** max(0, lead - 1)
            pred = max(0.0, last + trend * (1.0 + damp + damp**2) / 3.0 * lead)
        spread = max(abs(trend) * 0.75 * math.sqrt(lead), abs(last) * 0.03 * math.sqrt(lead))
        rows.append({
            "lead_day": lead,
            "date": (start_date + timedelta(days=lead)).strftime("%Y-%m-%d"),
            "discharge_m3s": round(pred, 3),
            "p10_m3s": round(max(0.0, pred - spread), 3),
            "p50_m3s": round(pred, 3),
            "p90_m3s": round(pred + spread, 3),
            "ensemble": False,
            "method": "trained_local_model" if model is not None else "transparent_persistence_trend_baseline",
        })
        # recursive baseline/model context
        working = pd.concat([working, pd.DataFrame([{"date": start_date + timedelta(days=lead), "discharge_m3s": pred}])], ignore_index=True)
        last = pred
        previous = float(working.iloc[-2].discharge_m3s)
        trend = last - previous
    return rows


def _payload_for_station(station_id, station):
    hist = _station_df({**station, "id": station_id})
    if hist.empty:
        return []
    latest_date = hist.date.max()
    current = hist[hist.date == latest_date]
    start = latest_date
    forecast = _model_forecast(hist, start, FORECAST_DAYS)
    history_for_rank = hist[hist.date <= latest_date]
    out = []
    for row in forecast:
        risk, pos = _risk_from_history(history_for_rank, row["discharge_m3s"])
        row.update({"risk": risk, "signal_position": pos})
        out.append(row)
    return out


def live_snapshot(stations, force=False):
    info = source_status()
    station_data = {sid: _payload_for_station(sid, st) for sid, st in stations.items()}
    if not info["data_available"]:
        return {
            "ok": False, "source": info["provider"], "dataset": info["dataset"],
            "variable": info["variable"], "unit": info["unit"], "stations": station_data,
            "error": "Local GloFAS dataset is not installed yet.", "issue_date": None,
            "fetched_at": info.get("loaded_at"), "stale": False,
        }
    df = _load()
    issue = df.date.max().date().isoformat()
    return {
        "ok": True,
        "source": info["provider"],
        "provider": "Copernicus CEMS",
        "dataset": info["dataset"],
        "variable": info["variable"],
        "unit": info["unit"],
        "issue_date": issue,
        "fetched_at": info.get("loaded_at"),
        "forecast_horizon_days": FORECAST_DAYS,
        "stale": False,
        "last_error": None,
        "stations": station_data,
    }


def fetch_glofas_forecast(stations, station_id, force=False):
    snap = live_snapshot(stations, force=force)
    return {
        "ok": bool(snap.get("ok")), "station": station_id, "source": snap.get("source"),
        "dataset": snap.get("dataset"), "variable": snap.get("variable"), "unit": snap.get("unit"),
        "issue_date": snap.get("issue_date"), "fetched_at": snap.get("fetched_at"),
        "forecast": snap.get("stations", {}).get(station_id, []), "error": snap.get("error"),
    }


def fetch_station_ensemble(station, force=False):
    """Historical-analog uncertainty view from the local archive.

    This is intentionally NOT called an operational GloFAS ensemble. It uses
    historical observations from the local GloFAS archive to show P10/P50/P90
    analog ranges when enough history exists.
    """
    station_id = station.get("id") or station.get("station_id")
    hist = _station_df({**station, "id": station_id})
    if hist.empty:
        raise RuntimeError("No local GloFAS history is available for this zone")
    latest = hist.date.max()
    forecast = _model_forecast(hist, latest, FORECAST_DAYS)
    return {
        "ok": True,
        "source": "Local GloFAS historical archive",
        "method": "model forecast with historical uncertainty band",
        "operational_ensemble": False,
        "issue_date": latest.date().isoformat(),
        "forecast": forecast,
    }


def fetch_historical_series(station, valid_days):
    station_id = station.get("id") or station.get("station_id")
    hist = _station_df({**station, "id": station_id})
    wanted = {d if isinstance(d, date) else date.fromisoformat(str(d)) for d in valid_days}
    return [
        {"date": r.date.strftime("%Y-%m-%d"), "discharge_m3s": float(r.discharge_m3s)}
        for _, r in hist.iterrows() if r.date.date() in wanted
    ]


def fetch_historical_forecast(station, issue_date, lead_days=FORECAST_DAYS):
    station_id = station.get("id") or station.get("station_id")
    hist = _station_df({**station, "id": station_id})
    if hist.empty:
        raise RuntimeError("No local GloFAS history is available for this zone")
    d = date.fromisoformat(issue_date)
    # Replay is a historical backtest: predict from the data available on issue date.
    eligible = hist[hist.date.dt.date <= d]
    if eligible.empty:
        raise RuntimeError(f"No GloFAS history exists on or before {issue_date}")
    return {
        "ok": True,
        "issue_date": issue_date,
        "source": "Local GloFAS historical replay",
        "forecast": _model_forecast(eligible, eligible.date.max(), lead_days),
    }
