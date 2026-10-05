"""FloodGuard GloFAS-only authentic data service.
No BWDB dependency and no synthetic fallback.
"""
from __future__ import annotations
import os, time, json, tempfile, threading
from datetime import datetime, timezone
import cdsapi
import xarray as xr

EWDS_API_URL=os.getenv("CDS_API_URL","https://ewds.climate.copernicus.eu/api").rstrip("/")
DATASET="cems-glofas-forecast"
VARIABLE="river_discharge_in_the_last_24_hours"
TTL=int(os.getenv("GLOFAS_CACHE_SECONDS","1800"))
BBOX=[27.5,88.0,20.5,93.0]  # Bangladesh + immediate upstream area, N,S,W,E
_lock=threading.Lock(); _cache={"payload":None,"expires":0,"error":None,"fetched_at":None}

def source_status():
    return {"authentic_only":True,"provider":"Copernicus CEMS / GloFAS","api":"EWDS CDS API","api_url":EWDS_API_URL,"dataset":DATASET,"variable":VARIABLE,"unit":"m3/s","forecast_horizon_days":30,"synthetic_fallback":False}

def _client():
    key=os.getenv("CDS_API_KEY","").strip()
    if not key: raise RuntimeError("CDS_API_KEY is not configured")
    return cdsapi.Client(url=EWDS_API_URL,key=key,quiet=True)

def _request_file():
    fd,path=tempfile.mkstemp(suffix=".nc"); os.close(fd)
    req={"system_version":"operational","hydrological_model":["lisflood"],"product_type":["ensemble_perturbed_forecasts"],"variable":[VARIABLE],"leadtime_hour":[str(h) for h in range(24,721,24)],"area":BBOX,"data_format":"netcdf","download_format":"unarchived"}
    _client().retrieve(DATASET,req).download(path)
    if not os.path.exists(path) or os.path.getsize(path)<1000: raise RuntimeError("EWDS returned an empty GloFAS file")
    return path

def _coords(ds):
    lat=next((n for n in ds.coords if n.lower() in ("latitude","lat")),None)
    lon=next((n for n in ds.coords if n.lower() in ("longitude","lon")),None)
    var=next((v for v in ds.data_vars if "discharge" in v.lower()),None)
    lead=next((n for n in ds.coords if "lead" in n.lower()),None)
    time=next((n for n in ds.coords if n.lower() in ("time","forecast_reference_time","valid_time")),None)
    realization=next((n for n in ds.dims if "real" in n.lower() or "ensemble" in n.lower() or "number" in n.lower()),None)
    if not lat or not lon or not var: raise RuntimeError("Unsupported GloFAS NetCDF structure")
    return lat,lon,var,lead,time,realization

def _series_for(ds,st):
    lat,lon,var,lead,time,realization=_coords(ds)
    da=ds[var].sel({lat:float(st['lat']),lon:float(st['lon'])},method="nearest")
    da=da.squeeze(drop=True)
    # Collapse non-time spatial dimensions and retain ensemble members when available.
    dims=list(da.dims)
    for d in list(dims):
        if d not in {lead,time,realization}: da=da.mean(d,skipna=True)
    if lead and lead in da.dims: da=da.sortby(lead)
    vals=[]
    if realization and realization in da.dims:
        q=da.quantile([.1,.5,.9],dim=realization,skipna=True)
        med=q.sel(quantile=.5); lo=q.sel(quantile=.1); hi=q.sel(quantile=.9)
        axis=lead or time
        for i in range(med.sizes[axis]):
            idx={axis:i};
            vals.append({"lead_day":i+1,"discharge_m3s":float(med.isel(idx).values),"p10_m3s":float(lo.isel(idx).values),"p90_m3s":float(hi.isel(idx).values),"ensemble":True})
    else:
        axis=lead or time
        arr=da.values.reshape(-1)
        for i,v in enumerate(arr[:30]):
            vals.append({"lead_day":i+1,"discharge_m3s":float(v),"p10_m3s":None,"p90_m3s":None,"ensemble":False})
    return vals

def fetch_all(stations,force=False):
    now=time.time()
    with _lock:
        if not force and _cache["payload"] and now<_cache["expires"]: return _cache["payload"]
        path=None
        try:
            path=_request_file()
            ds=xr.open_dataset(path)
            try:
                out={sid:_series_for(ds,st) for sid,st in stations.items()}
            finally: ds.close()
            payload={"ok":True,"source":"Copernicus CEMS / GloFAS operational forecast","variable":VARIABLE,"unit":"m3/s","fetched_at":datetime.now(timezone.utc).isoformat(),"stations":out}
            _cache.update(payload=payload,expires=now+TTL,error=None,fetched_at=payload["fetched_at"])
            return payload
        except Exception as exc:
            _cache["error"]=str(exc); _cache["expires"]=0
            raise
        finally:
            if path:
                try: os.remove(path)
                except OSError: pass

def fetch_glofas_forecast(stations,station_id,force=False):
    payload=fetch_all(stations,force=force)
    return {"ok":True,"station":station_id,**{k:payload[k] for k in ("source","variable","unit","fetched_at")},"forecast":payload["stations"].get(station_id,[])}

def live_snapshot(stations,force=False):
    return fetch_all(stations,force=force)

def fetch_historical_forecast(st, issue_date):
    """Retrieve the actual GloFAS operational forecast issued on a historical date."""
    key=os.getenv("CDS_API_KEY","").strip()
    if not key: raise RuntimeError("CDS_API_KEY is not configured")
    fd,path=tempfile.mkstemp(suffix=".nc"); os.close(fd)
    try:
        d=datetime.fromisoformat(issue_date).date()
        req={"system_version":"operational","hydrological_model":"lisflood","product_type":"control_forecast","variable":VARIABLE,"year":d.strftime('%Y'),"month":d.strftime('%m'),"day":d.strftime('%d'),"leadtime_hour":[str(h) for h in range(24,721,24)],"area":BBOX,"data_format":"netcdf","download_format":"unarchived"}
        _client().retrieve(DATASET,req).download(path)
        ds=xr.open_dataset(path)
        try: return {"ok":True,"issue_date":issue_date,"source":"Copernicus CEMS / GloFAS historical operational forecast replay","forecast":_series_for(ds,st)}
        finally: ds.close()
    finally:
        try: os.remove(path)
        except OSError: pass
