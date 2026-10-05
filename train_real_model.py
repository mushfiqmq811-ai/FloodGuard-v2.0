"""Train FloodGuard only from authentic BWDB observed water-level history.

No generated, interpolated, simulated or demo rows are permitted.
The resulting model predicts the next observed water level from real lagged observations.
"""
from __future__ import annotations
import json, os, shutil
from datetime import datetime, timezone, timedelta
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from joblib import dump

from real_data import fetch_bwdb_station_catalog, choose_station, fetch_bwdb_chart
from model import FEATURES, MODEL_PATH, META_PATH
from app import STATIONS


def build_rows():
    catalog=fetch_bwdb_station_catalog()
    rows=[]; mapping=[]; failures=[]
    for key, st in STATIONS.items():
        match=choose_station(catalog, st['station'], st['river'], st['lat'], st['lon'])
        if not match:
            failures.append({'station':key,'reason':'No defensible BWDB station match'})
            continue
        try:
            obs=fetch_bwdb_chart(match['chart_url'])
            mapping.append({'floodguard_station':key,'bwdb':match,'observations':len(obs)})
            parsed=[]
            for o in obs:
                try:
                    t=datetime.fromisoformat(o["observed_at"].replace("Z","+00:00"))
                    if t.tzinfo is None: t=t.replace(tzinfo=timezone.utc)
                    parsed.append((t,float(o["water_level_m"])))
                except Exception:
                    continue
            for i,(target_time,target) in enumerate(parsed):
                features=[]; ok=True
                for lag_h in (6,12,24):
                    cutoff=target_time-timedelta(hours=lag_h)
                    prior=[v for t,v in parsed[:i] if t <= cutoff]
                    if not prior:
                        ok=False; break
                    features.append(prior[-1])
                if ok and i > 0:
                    # [current, 6h lag, 12h lag, 24h lag], all observed before target.
                    rows.append({'station':key,'source_station':match['station_id'],'target_time':target_time.isoformat(),'x':[parsed[i-1][1],features[0],features[1],features[2]],'y':target})

        except Exception as exc:
            failures.append({'station':key,'reason':str(exc),'chart_url':match['chart_url']})
    return rows,mapping,failures


def main():
    rows,mapping,failures=build_rows()
    min_rows=int(os.getenv('REAL_MODEL_MIN_ROWS','100'))
    if len(rows)<min_rows:
        raise SystemExit(f'REAL_MODEL_TRAINING_ABORTED: only {len(rows)} authentic training rows available; need at least {min_rows}. No synthetic data will be created.')
    rows.sort(key=lambda r:r['target_time'])
    X=np.asarray([r['x'] for r in rows],dtype=float)
    y=np.asarray([r['y'] for r in rows],dtype=float)
    split=max(1,int(len(X)*0.8))
    if split>=len(X): raise SystemExit('Not enough rows for a chronological holdout.')
    model=RandomForestRegressor(n_estimators=300,max_depth=16,min_samples_leaf=2,random_state=42,n_jobs=-1)
    model.fit(X[:split],y[:split])
    pred=model.predict(X[split:])
    rmse=float(np.sqrt(mean_squared_error(y[split:],pred)))
    metrics={'mae_m':float(mean_absolute_error(y[split:],pred)),'rmse_m':rmse,'r2':float(r2_score(y[split:],pred)),'test_rows':int(len(y)-split)}
    MODEL_PATH.parent.mkdir(parents=True,exist_ok=True)
    dump(model,MODEL_PATH)
    meta={
        'model_name':'Random Forest — authentic BWDB-trained',
        'training_source':'BWDB Hydrology chart observations',
        'training_rows':int(len(rows)),
        'training_stations':sorted(set(r['station'] for r in rows)),
        'chronological_holdout':True,
        'metrics':metrics,
        'features':FEATURES,
        'trained_at_utc':datetime.now(timezone.utc).isoformat(),
        'synthetic_data_used':False,
        'simulation_fallback':False,
        'station_mapping':mapping,
        'source_failures':failures,
    }
    META_PATH.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'status':'REAL_MODEL_TRAINED','metrics':metrics,'mapping':mapping,'failures':failures},ensure_ascii=False,indent=2))

if __name__=='__main__': main()
