"""Authentic-data ingestion for FloodGuard BD.

No synthetic values are generated here. A source failure is represented as an
error/unavailable state. BWDB Hydrology chart pages are scraped for observed
water-level series; Copernicus EWDS/GloFAS is used for forecast/historical
river-discharge data.
"""
from __future__ import annotations
import json, os, re, sqlite3, time
from datetime import datetime, timezone
from difflib import SequenceMatcher
from html import unescape
from urllib.parse import urlencode
import requests

BWDB_AVAILABILITY_URL = os.getenv(
    "BWDB_AVAILABILITY_URL",
    "https://www.hydrology.bwdb.gov.bd/includes/water_level_data_available_print.php?dist=&river="
)
BWDB_RT_AVAILABILITY_URL = os.getenv(
    "BWDB_RT_AVAILABILITY_URL",
    "https://www.hydrology.bwdb.gov.bd/includes/water_level_rt_data_available_print.php?dist=&river="
)
BWDB_CHART_TEMPLATE = os.getenv(
    "BWDB_CHART_URL_TEMPLATE",
    "https://www.hydrology.bwdb.gov.bd/index.php?pagetitle=water_level&sub4={sl}&SUBID=131&id=125&id2=126&id3=308"
)
EWDS_API_URL = os.getenv("CDS_API_URL", "https://ewds.climate.copernicus.eu/api").rstrip("/")
GLOFAS_HISTORICAL_DATASET = "cems-glofas-historical"
STATION_CATALOG_TTL = int(os.getenv("BWDB_CATALOG_CACHE_SECONDS", "3600"))
TIMEOUT = int(os.getenv("SOURCE_HTTP_TIMEOUT", "25"))
USER_AGENT = "FloodGuard-BD/5.0 (research data collector)"


def _norm(v):
    return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())


def _clean_html_cell(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", unescape(s)).strip()


_CATALOG = {"value": None, "expires": 0.0}


def fetch_bwdb_station_catalog():
    """Read BWDB's published availability report; cache only the official response."""
    import time
    now = time.time()
    if _CATALOG["value"] is not None and now < _CATALOG["expires"]:
        return _CATALOG["value"]
    r = requests.get(BWDB_AVAILABILITY_URL, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    rows = []
    for raw in re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, re.I | re.S):
        cells = [_clean_html_cell(x) for x in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", raw, re.I | re.S)]
        if len(cells) < 9 or not re.match(r"^\d+$", cells[0]):
            continue
        try:
            rows.append({
                "sl": int(cells[0]), "station_id": cells[1], "station": cells[2], "river": cells[3],
                "tidal_status": cells[4], "district": cells[5], "upazila": cells[6],
                "lat": float(cells[7]), "lon": float(cells[8]),
                "first_date": cells[9] if len(cells) > 9 else None,
                "last_date": cells[10] if len(cells) > 10 else None,
            })
        except (ValueError, TypeError):
            continue
    if not rows:
        raise RuntimeError("BWDB station availability page returned no parseable stations")
    _CATALOG.update(value=rows, expires=now + STATION_CATALOG_TTL)
    return rows

def choose_station(catalog, target_station, target_river, target_lat, target_lon):
    """Choose a BWDB station by name/river first, then geography; never fabricate a station."""
    best, best_score = None, -1e9
    for s in catalog:
        name_score = SequenceMatcher(None, _norm(target_station), _norm(s["station"])).ratio()
        river_score = SequenceMatcher(None, _norm(target_river), _norm(s["river"])).ratio()
        dist = ((float(s["lat"])-target_lat)**2 + (float(s["lon"])-target_lon)**2) ** 0.5
        geo_score = max(0.0, 1.0 - dist / 2.0)
        score = name_score * 0.55 + river_score * 0.30 + geo_score * 0.15
        if score > best_score:
            best, best_score = s, score
    if not best or best_score < 0.48:
        return None
    return {**best, "match_score": round(best_score, 4), "chart_url": BWDB_CHART_TEMPLATE.format(sl=best["sl"])}


def _extract_pairs_from_js(text):
    """Extract common Highcharts/Chart.js/Google-chart style date/value arrays."""
    pairs = []
    # Explicit [date,value] rows.
    for m in re.finditer(r"\[\s*['\"]?(\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)['\"]?\s*,\s*(-?\d+(?:\.\d+)?)\s*\]", text):
        pairs.append((m.group(1), float(m.group(2))))
    # Common JS objects: {date:'...', value:7.2} / {x:'...', y:7.2}
    for m in re.finditer(r"\{[^{}]{0,300}?(?:date|time|datetime|x)\s*:\s*['\"]([^'\"]+)['\"][^{}]{0,200}?(?:value|level|water_level|waterlevel|y)\s*:\s*(-?\d+(?:\.\d+)?)[^{}]*\}", text, re.I):
        pairs.append((m.group(1), float(m.group(2))))
    return pairs


def parse_bwdb_chart(text):
    """Parse an authentic BWDB chart page. Supports tables, embedded rows and common JS chart payloads."""
    pairs = _extract_pairs_from_js(text)
    # HTML table records.
    for raw in re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.I | re.S):
        cells = [_clean_html_cell(x) for x in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", raw, re.I | re.S)]
        if not cells: continue
        date_idx = next((i for i,c in enumerate(cells) if re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", c)), None)
        if date_idx is None: continue
        nums=[]
        for c in cells[date_idx+1:]:
            try: nums.append(float(c.replace(",", "")))
            except ValueError: pass
        if nums: pairs.append((cells[date_idx], nums[-1]))
    # De-duplicate and validate physical numeric bounds without inventing values.
    out=[]; seen=set()
    for ts, level in pairs:
        key=(str(ts).strip(), round(float(level), 6))
        if key in seen or not (-20 < float(level) < 100): continue
        seen.add(key)
        try:
            dt = datetime.fromisoformat(str(ts).replace("/", "-").replace("Z", "+00:00"))
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            iso = dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            iso = str(ts).strip()
        out.append({"observed_at": iso, "water_level_m": float(level)})
    out.sort(key=lambda x: x["observed_at"])
    return out


def fetch_bwdb_chart(url):
    r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    rows = parse_bwdb_chart(r.text)
    if not rows:
        raise RuntimeError("BWDB chart page was reachable but no chart/table observations could be parsed")
    return rows


def fetch_glofas_historical_daily(lat, lon, start_date, end_date, output_dir):
    """Download real GloFAS historical data through the current EWDS API client.

    This function intentionally fails if credentials/licence access are missing; it never
    substitutes artificial discharge values.
    """
    key = os.getenv("CDS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("CDS_API_KEY is required for authentic GloFAS historical data")
    try:
        import cdsapi
    except ImportError as exc:
        raise RuntimeError("cdsapi is required for authentic GloFAS historical data") from exc
    os.makedirs(output_dir, exist_ok=True)
    target = os.path.join(output_dir, f"glofas_{start_date}_{end_date}_{lat:.3f}_{lon:.3f}.grib")
    if os.path.exists(target) and os.path.getsize(target) > 0:
        return target
    client = cdsapi.Client(url=EWDS_API_URL, key=key, quiet=True)
    req = {
        "system_version": [os.getenv("GLOFAS_HISTORICAL_SYSTEM_VERSION", "version_5_0")],
        "hydrological_model": ["lisflood"],
        "product_type": ["consolidated"],
        "variable": ["river_discharge_in_the_last_24_hours"],
        "hyear": sorted(set(d[:4] for d in _date_range(start_date, end_date))),
        "hmonth": sorted(set(d[5:7] for d in _date_range(start_date, end_date))),
        "hday": sorted(set(d[8:10] for d in _date_range(start_date, end_date))),
        "data_format": "grib2",
        "download_format": "unarchived",
        "geographical_area": [float(lat)+0.05, float(lon)-0.05, float(lat)-0.05, float(lon)+0.05],
    }
    client.retrieve(GLOFAS_HISTORICAL_DATASET, req, target)
    if not os.path.exists(target) or os.path.getsize(target) == 0:
        raise RuntimeError("GloFAS historical request completed without a data file")
    return target


def _date_range(start_date, end_date):
    from datetime import date, timedelta
    a=date.fromisoformat(start_date); b=date.fromisoformat(end_date)
    while a <= b:
        yield a.isoformat(); a += timedelta(days=1)


def source_status():
    return {
        "bwdb_chart_source": BWDB_AVAILABILITY_URL,
        "bwdb_chart_template": BWDB_CHART_TEMPLATE,
        "glofas_api": EWDS_API_URL,
        "glofas_dataset": GLOFAS_HISTORICAL_DATASET,
        "authentic_only": True,
    }
