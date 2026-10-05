"""Live authentic-source pipeline. No synthetic fallback."""
from datetime import datetime,timezone
import os,re,threading,time
from real_data import fetch_bwdb_station_catalog,choose_station,fetch_bwdb_chart
CACHE={}; LOCK=threading.Lock(); TTL=int(os.getenv("LIVE_REFRESH_SECONDS","300"))

def norm(x): return re.sub(r"[^a-z0-9]+","",str(x or "").lower())
def _match(st):
    return choose_station(fetch_bwdb_station_catalog(),st['station'],st['river'],st['lat'],st['lon'])
def fetch_ffwc_current(stations=None):
    stations=stations or {}; data={}; failures=[]
    for key,st in stations.items():
        try:
            m=_match(st)
            if not m: raise RuntimeError(f"No defensible BWDB station match for {st['name']}")
            cache_key=m['station_id']; now=time.time()
            with LOCK: c=CACHE.get(cache_key)
            if c and now-c['at']<TTL: rows=c['rows']
            else:
                rows=fetch_bwdb_chart(m['chart_url'])
                with LOCK: CACHE[cache_key]={"at":now,"rows":rows}
            if len(rows)<1: raise RuntimeError("No observations returned")
            latest=rows[-1]; prev=rows[-2] if len(rows)>1 else None
            data[(norm(st['river']),norm(st['station']))]={"river":st['river'],"station":st['station'],"current":latest['water_level_m'],"observed_at":latest['observed_at'],"source":"BWDB Hydrology chart observation","source_url":m['chart_url'],"live":True,"simulation":False,"snapshot":False,"danger":st.get('danger'),"previous_level":prev['water_level_m'] if prev else None,"bwdb_station_id":m['station_id'],"bwdb_match_score":m['match_score'],"history":[{"time":r['observed_at'],"level":r['water_level_m'],"kind":"BWDB observed"} for r in rows]}
        except Exception as e: failures.append({"station":key,"error":str(e)})
    if not data: raise RuntimeError("No authentic BWDB chart observations could be fetched: "+str(failures[:3]))
    return {"ok":True,"source":"BWDB Hydrology chart observations","source_url":"https://www.hydrology.bwdb.gov.bd/","fetched_at":datetime.now(timezone.utc).isoformat(),"data":data,"failures":failures}
