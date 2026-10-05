"""FloodGuard BD — authentic Copernicus GloFAS data layer.

Production rules:
- GloFAS is the only hydrological source.
- No synthetic/fake hydrological fallback is ever generated.
- Forecast issue dates are selected automatically, newest first.
- A successful real snapshot is persisted locally so a process restart does not
  erase the last authentic dataset.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import cdsapi
import xarray as xr

EWDS_API_URL = os.getenv("CDS_API_URL", "https://ewds.climate.copernicus.eu/api").rstrip("/")
DATASET = "cems-glofas-forecast"
HISTORICAL_DATASET = "cems-glofas-historical"
VARIABLE = "river_discharge_in_the_last_24_hours"
SYSTEM_VERSION = "operational"
HYDRO_MODEL = "lisflood"
CACHE_TTL = int(os.getenv("GLOFAS_CACHE_SECONDS", "1800"))
FORECAST_DAYS = int(os.getenv("GLOFAS_FORECAST_DAYS", "15"))
# N,S,W,E — small enough to keep the EWDS request practical while covering
# Bangladesh and immediate upstream areas used by the strategic zones.
BBOX = [27.5, 88.0, 20.5, 93.0]
CACHE_FILE = Path(os.getenv("GLOFAS_SNAPSHOT_FILE", Path(__file__).with_name("cache") / "glofas_snapshot.json"))

_lock = threading.RLock()
_cache = {"payload": None, "expires": 0.0, "error": None, "fetched_at": None, "issue_date": None}


def source_status():
    return {
        "authentic_only": True,
        "provider": "Copernicus CEMS / GloFAS",
        "api": "EWDS CDS API",
        "api_url": EWDS_API_URL,
        "dataset": DATASET,
        "variable": VARIABLE,
        "unit": "m3/s",
        "forecast_horizon_days": FORECAST_DAYS,
        "operational_model": "LISFLOOD",
        "glofas_version": "Operational",
        "synthetic_fallback": False,
        "persisted_real_snapshot": str(CACHE_FILE),
    }


def _load_persisted():
    if _cache["payload"] is not None or not CACHE_FILE.exists():
        return
    try:
        obj = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if obj.get("ok") and obj.get("stations"):
            _cache.update(
                payload=obj,
                expires=0.0,
                error=None,
                fetched_at=obj.get("fetched_at"),
                issue_date=obj.get("issue_date"),
            )
    except Exception:
        # A corrupt cache must never become fake data.
        pass


_load_persisted()


def _persist(payload):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(CACHE_FILE)


def _client():
    key = os.getenv("CDS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("CDS_API_KEY is not configured")
    return cdsapi.Client(url=EWDS_API_URL, key=key, quiet=True)


def _lead_hours():
    return [str(h) for h in range(24, (FORECAST_DAYS + 1) * 24, 24)]


def _candidate_issue_dates():
    """Newest-first issue dates; daily operational publication can lag UTC."""
    today = datetime.now(timezone.utc).date()
    lookback = int(os.getenv("GLOFAS_ISSUE_LOOKBACK_DAYS", "5"))
    return [today - timedelta(days=i) for i in range(lookback + 1)]


def _request_file(issue_date: date, product_type="control_forecast", bbox=None, days=None):
    fd, path = tempfile.mkstemp(suffix=".nc")
    os.close(fd)
    request = {
        "system_version": SYSTEM_VERSION,
        "hydrological_model": HYDRO_MODEL,
        "product_type": product_type,
        "variable": VARIABLE,
        "year": issue_date.strftime("%Y"),
        "month": issue_date.strftime("%m"),
        "day": issue_date.strftime("%d"),
        "leadtime_hour": [str(h) for h in range(24, ((days or FORECAST_DAYS) + 1) * 24, 24)],
        "area": bbox or BBOX,
        "data_format": "netcdf",
        "download_format": "unarchived",
    }
    _client().retrieve(DATASET, request).download(path)
    if not os.path.exists(path) or os.path.getsize(path) < 1000:
        raise RuntimeError("EWDS returned an empty GloFAS file")
    return path


def _coord_name(ds, candidates):
    names = list(ds.coords) + list(ds.dims)
    lower = {n.lower(): n for n in names}
    for candidate in candidates:
        if candidate in lower:
            return lower[candidate]
    for n in names:
        nl = n.lower()
        if any(c in nl for c in candidates):
            return n
    return None


def _to_float(v):
    try:
        x=float(v)
        return x if x == x and abs(x) != float("inf") else None
    except Exception:
        return None


def _coords(ds):
    """Resolve GloFAS NetCDF coordinates across current/legacy NetCDF layouts."""
    lat = _coord_name(ds, ("latitude", "lat"))
    lon = _coord_name(ds, ("longitude", "lon"))
    var = next((v for v in ds.data_vars if v.lower() in ("dis24", "river_discharge_in_the_last_24_hours") or "discharge" in v.lower()), None)
    step = _coord_name(ds, ("leadtime_hour", "leadtime", "step", "forecast_period"))
    number = _coord_name(ds, ("number", "realization", "ensemble", "member"))
    time_name = _coord_name(ds, ("forecast_reference_time", "time", "valid_time"))
    if not lat or not lon or not var:
        raise RuntimeError(f"Unsupported GloFAS NetCDF structure: coords={list(ds.coords)}, vars={list(ds.data_vars)}")
    return lat, lon, var, step, number, time_name


def _lead_sort_value(v):
    try:
        if hasattr(v, "astype"):
            # numpy timedelta64 -> hours
            x = v.astype("timedelta64[h]").astype("int64")
            return float(x)
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        text=str(v).lower()
        m=re.search(r"([0-9]+(?:\\.[0-9]+)?)", text)
        return float(m.group(1)) if m else 0.0


def _series_for(ds, station, max_days=None):
    """Extract exactly one nearest GloFAS river-grid forecast series.

    Handles the common GloFAS layouts where a one-element forecast-reference
    time dimension sits beside a lead-time/step dimension. Never flattens an
    unresolved time/member axis into fake sequential days.
    """
    lat, lon, var, step, number, time_name = _coords(ds)
    da = ds[var].sel({lat: float(station["lat"]), lon: float(station["lon"])}, method="nearest")
    limit = int(max_days or FORECAST_DAYS)

    # Remove singleton reference-time dimensions first. A reference time with
    # more than one value is not a valid single forecast issue for this parser.
    for d in list(da.dims):
        if d == time_name and da.sizes.get(d, 1) == 1:
            da = da.isel({d: 0}, drop=True)
    # Drop other singleton dimensions safely.
    for d in list(da.dims):
        if da.sizes.get(d, 1) == 1 and d not in (step, number):
            da = da.isel({d: 0}, drop=True)

    # Re-resolve lead dimension after singleton removal.
    lead_dim = step if step and step in da.dims else None
    if lead_dim is None:
        # Prefer a coordinate/dimension whose name clearly means lead time.
        for d in da.dims:
            dl=d.lower()
            if any(k in dl for k in ("lead", "step", "forecast_period")):
                lead_dim=d; break
    if lead_dim is None:
        # A control forecast should have exactly one non-spatial temporal axis.
        candidates=[d for d in da.dims if d not in (number, time_name)]
        if len(candidates)==1 and da.sizes[candidates[0]]>1:
            lead_dim=candidates[0]

    if lead_dim is None or lead_dim not in da.dims:
        raise RuntimeError(f"GloFAS forecast has no resolvable lead-time dimension; dims={dict(da.sizes)}")

    # If a non-singleton reference-time axis remains, do not silently flatten it.
    for d in list(da.dims):
        if d in (lead_dim, number):
            continue
        if d == time_name:
            raise RuntimeError(f"GloFAS forecast has unresolved reference-time dimension {d}={da.sizes[d]}")
        if da.sizes[d] == 1:
            da=da.isel({d:0},drop=True)
        else:
            # Any remaining spatial/unknown dimension should already have been
            # selected; averaging it would fabricate a station value.
            raise RuntimeError(f"GloFAS forecast has unresolved dimension {d}={da.sizes[d]}")

    # Sort lead times using actual coordinate values. GloFAS uses daily output.
    try:
        vals=list(da[lead_dim].values)
        order=sorted(range(len(vals)), key=lambda i:_lead_sort_value(vals[i]))
        da=da.isel({lead_dim:order})
    except Exception:
        pass

    n=min(limit, int(da.sizes[lead_dim]))
    if number and number in da.dims:
        q=da.quantile([0.10,0.50,0.90], dim=number, skipna=True)
        out=[]
        for i in range(n):
            p10=_to_float(q.sel(quantile=0.10).isel({lead_dim:i}).values)
            p50=_to_float(q.sel(quantile=0.50).isel({lead_dim:i}).values)
            p90=_to_float(q.sel(quantile=0.90).isel({lead_dim:i}).values)
            if p50 is not None:
                out.append({"lead_day":i+1,"discharge_m3s":p50,"p10_m3s":p10,"p50_m3s":p50,"p90_m3s":p90,"ensemble":True})
        return out

    out=[]
    for i in range(n):
        x=_to_float(da.isel({lead_dim:i}).values)
        if x is not None:
            out.append({"lead_day":i+1,"discharge_m3s":x,"p10_m3s":None,"p50_m3s":x,"p90_m3s":None,"ensemble":False})
    if len(out)<min(3,limit):
        raise RuntimeError(f"Only {len(out)} valid GloFAS lead days were parsed; expected {min(3,limit)}+")
    return out

def _build_payload(issue_date, ds, stations):
    station_data = {}
    for sid, st in stations.items():
        series = _series_for(ds, st)
        if not series:
            raise RuntimeError(f"No GloFAS river-discharge series found near {st['lat']},{st['lon']} for {sid}")
        station_data[sid] = series
    return {
        "ok": True,
        "source": "Copernicus CEMS / GloFAS operational forecast",
        "provider": "Copernicus CEMS",
        "dataset": DATASET,
        "variable": VARIABLE,
        "unit": "m3/s",
        "issue_date": issue_date.isoformat(),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "forecast_horizon_days": FORECAST_DAYS,
        "stale": False,
        "last_error": None,
        "stations": station_data,
    }


def fetch_all(stations, force=False):
    with _lock:
        _load_persisted()
        now = time.time()
        if not force and _cache["payload"] and now < _cache["expires"]:
            return _cache["payload"]

        last_error = None
        for issue_date in _candidate_issue_dates():
            path = None
            try:
                path = _request_file(issue_date, product_type="control_forecast")
                ds = xr.open_dataset(path, chunks=None)
                try:
                    payload = _build_payload(issue_date, ds, stations)
                finally:
                    ds.close()
                _persist(payload)
                _cache.update(payload=payload, expires=time.time() + CACHE_TTL, error=None, fetched_at=payload["fetched_at"], issue_date=payload["issue_date"])
                return payload
            except Exception as exc:
                last_error = f"{issue_date.isoformat()}: {exc}"
            finally:
                if path:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

        _cache["error"] = last_error or "No GloFAS issue date could be fetched"
        # Keep an older authentic snapshot if one exists. Never manufacture data.
        if _cache["payload"]:
            cached=dict(_cache["payload"]); cached['stale']=True; cached['last_error']=last_error
            return cached
        raise RuntimeError(_cache["error"])


_ENSEMBLE_CACHE = {}
_ENSEMBLE_LOCK = threading.RLock()

def fetch_station_ensemble(station, force=False):
    """Fetch a small, station-local GloFAS ensemble payload for Research Mode.
    This deliberately does NOT download the whole Bangladesh ensemble grid.
    """
    key=f"{station["lat"]:.3f},{station["lon"]:.3f}"
    with _ENSEMBLE_LOCK:
        cached=_ENSEMBLE_CACHE.get(key)
        if cached and not force and time.time()-cached.get("_ts",0) < CACHE_TTL:
            return cached["payload"]
        last_error=None
        pad=float(os.getenv("GLOFAS_RESEARCH_BBOX_PAD","0.10"))
        bbox=[station["lat"]+pad, station["lon"]-pad, station["lat"]-pad, station["lon"]+pad]
        for issue_date in _candidate_issue_dates():
            path=None
            try:
                path=_request_file(issue_date, product_type="ensemble_perturbed_forecasts", bbox=bbox, days=FORECAST_DAYS)
                ds=xr.open_dataset(path, chunks=None)
                try:
                    series=_series_for(ds, station)
                finally:
                    ds.close()
                if not series:
                    raise RuntimeError("No ensemble discharge series returned for this research zone")
                payload={"ok":True,"source":"Copernicus CEMS / GloFAS operational ensemble","dataset":DATASET,"issue_date":issue_date.isoformat(),"fetched_at":datetime.now(timezone.utc).isoformat(),"forecast":series}
                _ENSEMBLE_CACHE[key]={"_ts":time.time(),"payload":payload}
                return payload
            except Exception as exc:
                last_error=f"{issue_date.isoformat()}: {exc}"
            finally:
                if path:
                    try: os.remove(path)
                    except OSError: pass
        raise RuntimeError(last_error or "GloFAS ensemble data unavailable")

def fetch_glofas_forecast(stations, station_id, force=False):
    if station_id not in stations:
        raise KeyError(f"Unknown station: {station_id}")
    payload = fetch_all(stations, force=force)
    return {
        "ok": True,
        "station": station_id,
        "source": payload["source"],
        "dataset": payload["dataset"],
        "variable": payload["variable"],
        "unit": payload["unit"],
        "issue_date": payload.get("issue_date"),
        "fetched_at": payload.get("fetched_at"),
        "forecast": payload["stations"].get(station_id, []),
    }


def live_snapshot(stations, force=False):
    return fetch_all(stations, force=force)


def _historical_request_file(days, bbox):
    fd, path = tempfile.mkstemp(suffix=".nc")
    os.close(fd)
    request = {
        "system_version": ["version_5_0"],
        "hydrological_model": ["lisflood"],
        "product_type": ["intermediate"],
        "variable": [VARIABLE],
        "hyear": sorted({d.strftime("%Y") for d in days}),
        "hmonth": sorted({d.strftime("%m") for d in days}),
        "hday": sorted({d.strftime("%d") for d in days}),
        "area": bbox,
        "data_format": "netcdf",
        "download_format": "unarchived",
    }
    _client().retrieve(HISTORICAL_DATASET, request).download(path)
    if not os.path.exists(path) or os.path.getsize(path) < 1000:
        raise RuntimeError("EWDS returned an empty historical GloFAS file")
    return path


def fetch_historical_series(station, valid_days):
    """Fetch modelled GloFAS historical discharge for selected valid dates.

    This is a GloFAS-vs-GloFAS verification target, not independent gauge truth.
    """
    days = [d if isinstance(d, date) else date.fromisoformat(str(d)) for d in valid_days]
    if not days:
        return []
    pad = 0.06
    bbox = [station["lat"] + pad, station["lon"] - pad, station["lat"] - pad, station["lon"] + pad]
    path = None
    try:
        path = _historical_request_file(days, bbox)
        ds = xr.open_dataset(path)
        try:
            lat, lon, var, _, _, time_name = _coords(ds)
            da = ds[var].sel({lat: float(station["lat"]), lon: float(station["lon"])}, method="nearest")
            # Historical product is daily; normalize time axis to YYYY-MM-DD.
            tname = time_name or _coord_name(ds, ("time",))
            if not tname or tname not in da.dims:
                raise RuntimeError("Historical GloFAS file has no time coordinate")
            vals = da.values.reshape(-1)
            times = ds[tname].values.reshape(-1)
            rows = []
            for t, v in zip(times, vals):
                x = _to_float(v)
                if x is not None:
                    ts = str(t)[:10]
                    rows.append({"date": ts, "discharge_m3s": x})
            wanted = {d.isoformat() for d in days}
            return [r for r in rows if r["date"] in wanted]
        finally:
            ds.close()
    finally:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def fetch_historical_forecast(station, issue_date, lead_days=FORECAST_DAYS):
    """Retrieve an operational GloFAS forecast issued on a historical date."""
    d = date.fromisoformat(issue_date)
    fd, path = tempfile.mkstemp(suffix=".nc")
    os.close(fd)
    try:
        request = {
            "system_version": SYSTEM_VERSION,
            "hydrological_model": HYDRO_MODEL,
            "product_type": "control_forecast",
            "variable": VARIABLE,
            "year": d.strftime("%Y"),
            "month": d.strftime("%m"),
            "day": d.strftime("%d"),
            "leadtime_hour": [str(h) for h in range(24, (lead_days + 1) * 24, 24)],
            "area": [station["lat"] + 0.06, station["lon"] - 0.06, station["lat"] - 0.06, station["lon"] + 0.06],
            "data_format": "netcdf",
            "download_format": "unarchived",
        }
        _client().retrieve(DATASET, request).download(path)
        ds = xr.open_dataset(path)
        try:
            series = _series_for(ds, station)
        finally:
            ds.close()
        return {"ok": True, "issue_date": issue_date, "source": "Copernicus CEMS / GloFAS operational forecast replay", "forecast": series}
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
