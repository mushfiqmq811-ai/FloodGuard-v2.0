"""Authentic BWDB Hydrology + Copernicus EWDS ingestion.

Rules:
- Never fabricate observations or forecasts.
- BWDB water levels are observations only when extracted from an official chart/data response.
- GloFAS is river discharge forecast data; it is never relabelled as observed stage.
"""
from __future__ import annotations
import json, os, re, time
from datetime import datetime, timezone
from difflib import SequenceMatcher
from urllib.parse import urljoin, urlparse
import requests

BWDB_BASE="https://www.hydrology.bwdb.gov.bd/"
BWDB_AVAILABILITY_URL=os.getenv("BWDB_AVAILABILITY_URL",BWDB_BASE+"includes/water_level_data_available_print.php?dist=&river=")
BWDB_RT_AVAILABILITY_URL=os.getenv("BWDB_RT_AVAILABILITY_URL",BWDB_BASE+"includes/water_level_rt_data_available_print.php?dist=&river=")
BWDB_CHART_TEMPLATE=os.getenv("BWDB_CHART_URL_TEMPLATE",BWDB_BASE+"index.php?pagetitle=water_level&sub4={sl}&SUBID=131&id=125&id2=126&id3=308")
EWDS_API_URL=os.getenv("CDS_API_URL","https://ewds.climate.copernicus.eu/api").rstrip("/")
TIMEOUT=int(os.getenv("SOURCE_HTTP_TIMEOUT","25"))
UA="FloodGuard-BD/6.0 (research data collector)"
_CATALOG={"value":None,"expires":0.0}


def _norm(v): return re.sub(r"[^a-z0-9]+","",str(v or "").lower())
def _clean(s): return re.sub(r"\s+"," ",re.sub(r"<[^>]+>"," ",s or "")).strip()

def _get(url, **kw):
    h={"User-Agent":UA,"Accept":"text/html,application/json,*/*"}; h.update(kw.pop("headers",{}))
    r=requests.get(url,timeout=TIMEOUT,headers=h,**kw); r.raise_for_status(); return r

def fetch_bwdb_station_catalog(force=False):
    now=time.time()
    if not force and _CATALOG["value"] and now<_CATALOG["expires"]: return _CATALOG["value"]
    r=_get(BWDB_AVAILABILITY_URL)
    rows=[]
    for raw in re.findall(r"<tr[^>]*>(.*?)</tr>",r.text,re.I|re.S):
        cells=[_clean(x) for x in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>",raw,re.I|re.S)]
        if len(cells)<9 or not cells[0].isdigit(): continue
        try:
            rows.append({"sl":int(cells[0]),"station_id":cells[1],"station":cells[2],"river":cells[3],"tidal_status":cells[4],"district":cells[5],"upazila":cells[6],"lat":float(cells[7]),"lon":float(cells[8]),"first_date":cells[9] if len(cells)>9 else None,"last_date":cells[10] if len(cells)>10 else None})
        except (ValueError,TypeError): pass
    if not rows: raise RuntimeError("BWDB availability report returned no parseable stations")
    _CATALOG.update(value=rows,expires=now+int(os.getenv("BWDB_CATALOG_CACHE_SECONDS","3600")))
    return rows

def choose_station(catalog,target_station,target_river,target_lat,target_lon):
    best=None; best_score=-1
    for s in catalog:
        ns=SequenceMatcher(None,_norm(target_station),_norm(s["station"])).ratio()
        nr=SequenceMatcher(None,_norm(target_river),_norm(s["river"])).ratio()
        d=((s["lat"]-target_lat)**2+(s["lon"]-target_lon)**2)**0.5
        geo=max(0,1-d/2)
        score=.55*ns+.30*nr+.15*geo
        if score>best_score: best,best_score=s,score
    if not best or best_score<.48: return None
    return {**best,"match_score":round(best_score,4),"chart_url":BWDB_CHART_TEMPLATE.format(sl=best["sl"])}

def _parse_date(s):
    s=str(s).strip().replace("/","-")
    for fmt in (None,"%d-%m-%Y","%d-%m-%Y %H:%M","%d-%m-%Y %H:%M:%S","%Y-%m-%d","%Y-%m-%d %H:%M","%Y-%m-%d %H:%M:%S"):
        try:
            dt=datetime.fromisoformat(s) if fmt is None else datetime.strptime(s,fmt)
            if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception: pass
    return None

def _pairs(text):
    out=[]
    # JSON/JS pair objects and arrays, including numeric timestamps.
    patterns=[
      r"[\[\(]\s*['\"]?([^'\"\]\),]+?)['\"]?\s*[,;:]\s*(-?\d+(?:\.\d+)?)\s*[\]\)]",
      r"(?:date|datetime|time|timestamp|x)\s*[:=]\s*['\"]([^'\"]+)['\"][\s,}]{0,200}(?:value|level|water[_ ]?level|waterlevel|y)\s*[:=]\s*(-?\d+(?:\.\d+)?)",
      r"(?:value|level|water[_ ]?level|waterlevel|y)\s*[:=]\s*(-?\d+(?:\.\d+)?)[\s,}]{0,200}(?:date|datetime|time|timestamp|x)\s*[:=]\s*['\"]([^'\"]+)['\"]"
    ]
    for i,p in enumerate(patterns):
        for m in re.finditer(p,text,re.I):
            a,b=m.group(1),m.group(2)
            if i==2: a,b=b,a
            ts=_parse_date(a)
            if ts:
                try: v=float(b); out.append((ts,v))
                except ValueError: pass
    return out

def parse_bwdb_chart(text):
    pairs=_pairs(text)
    # HTML tables with a date-like cell followed by a numeric level.
    for raw in re.findall(r"<tr[^>]*>(.*?)</tr>",text,re.I|re.S):
        cells=[_clean(x) for x in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>",raw,re.I|re.S)]
        for i,c in enumerate(cells):
            ts=_parse_date(c)
            if not ts: continue
            for n in reversed(cells[i+1:]):
                try:
                    v=float(n.replace(",",""))
                    if -20<v<100: pairs.append((ts,v)); break
                except ValueError: pass
    seen=set(); out=[]
    for ts,v in sorted(pairs):
        key=(ts,round(v,6))
        if key in seen or not (-20<v<100): continue
        seen.add(key); out.append({"observed_at":ts,"water_level_m":float(v)})
    return out

def _discover_embedded_urls(page_url,text):
    urls=[]
    for m in re.finditer(r"(?:src|href)\s*=\s*['\"]([^'\"]+)['\"]",text,re.I):
        u=urljoin(page_url,m.group(1))
        low=u.lower()
        if any(k in low for k in ("water","level","chart","graph","ajax","data")): urls.append(u)
    for m in re.finditer(r"(?:url|endpoint|ajax|dataUrl|dataurl)\s*[:=]\s*['\"]([^'\"]+)['\"]",text,re.I):
        urls.append(urljoin(page_url,m.group(1)))
    return list(dict.fromkeys(urls))[:30]

def fetch_bwdb_chart(url):
    r=_get(url); rows=parse_bwdb_chart(r.text)
    if rows: return rows
    # Many BWDB pages render the chart from a secondary JS/AJAX payload.
    for u in _discover_embedded_urls(url,r.text):
        try:
            rr=_get(u,headers={"Referer":url,"Accept":"application/json,text/plain,*/*"})
            rows=parse_bwdb_chart(rr.text)
            if rows: return rows
        except Exception: continue
    # Some pages expose arrays of labels and values separately.
    labels=re.findall(r"(?:categories|labels)\s*[:=]\s*\[([^\]]+)\]",r.text,re.I|re.S)
    values=re.findall(r"(?:data|values)\s*[:=]\s*\[([^\]]+)\]",r.text,re.I|re.S)
    for la,va in zip(labels,values):
        ls=[x.strip().strip("'\"") for x in la.split(",")]; vs=re.findall(r"-?\d+(?:\.\d+)?",va)
        for a,b in zip(ls,vs):
            ts=_parse_date(a)
            if ts: rows.append({"observed_at":ts,"water_level_m":float(b)})
    if not rows: raise RuntimeError("BWDB chart page reachable but no observation payload could be parsed")
    return sorted(rows,key=lambda x:x["observed_at"])

def fetch_glofas_forecast_file(station,target_dir="/tmp"):
    key=os.getenv("CDS_API_KEY","").strip()
    if not key: raise RuntimeError("CDS_API_KEY is not configured")
    import cdsapi
    import tempfile
    os.makedirs(target_dir,exist_ok=True)
    target=os.path.join(target_dir,f"glofas_{station['id']}.nc")
    client=cdsapi.Client(url=EWDS_API_URL,key=key,quiet=True)
    req={"system_version":["operational"],"hydrological_model":["lisflood"],"product_type":["control_forecast"],"variable":["river_discharge_in_the_last_24_hours"],"leadtime_hour":[str(h) for h in range(24,721,24)],"geographical_area":[station['lat']+.1,station['lon']-.1,station['lat']-.1,station['lon']+.1],"data_format":"netcdf4","download_format":"unarchived"}
    client.retrieve("cems-glofas-forecast",req,target)
    if not os.path.exists(target) or os.path.getsize(target)==0: raise RuntimeError("GloFAS request returned no file")
    return target

def extract_glofas_series(path,lat,lon):
    import xarray as xr
    ds=xr.open_dataset(path)
    try:
        lat_name=next((n for n in ds.coords if n.lower() in ("latitude","lat")),None)
        lon_name=next((n for n in ds.coords if n.lower() in ("longitude","lon")),None)
        time_name=next((n for n in ds.coords if "time" in n.lower()),None)
        var=next((v for v in ds.data_vars if "discharge" in v.lower()),None)
        if not all((lat_name,lon_name,time_name,var)): raise RuntimeError("Unrecognized GloFAS NetCDF structure")
        da=ds[var].sel({lat_name:lat,lon_name:lon},method="nearest").squeeze()
        vals=[]
        for t,v in zip(ds[time_name].values,da.values):
            try: vals.append({"forecast_time":str(t),"discharge_m3s":float(v)})
            except Exception: pass
        return vals
    finally: ds.close()

def source_status():
    return {"authentic_only":True,"bwdb_availability_url":BWDB_AVAILABILITY_URL,"bwdb_chart_template":BWDB_CHART_TEMPLATE,"glofas_api":EWDS_API_URL,"glofas_forecast_dataset":"cems-glofas-forecast","glofas_variable":"river_discharge_in_the_last_24_hours"}
