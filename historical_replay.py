"""Historical GloFAS forecast replay scaffolding using the official operational forecast dataset.

Important: this replays forecasts issued on the historical issue date; it does not use
post-event observations as forecast inputs. Ground truth must come from BWDB observations.
"""
from __future__ import annotations
import os, tempfile
from datetime import date
from real_data import EWDS_API_URL, extract_glofas_series

def fetch_historical_forecast(issue_date, station, lead_days=15, target_dir='/tmp'):
    import cdsapi
    d=date.fromisoformat(issue_date)
    key=os.getenv('CDS_API_KEY','').strip()
    if not key: raise RuntimeError('CDS_API_KEY is not configured')
    os.makedirs(target_dir,exist_ok=True)
    path=os.path.join(target_dir,f"glofas_issue_{issue_date}_{station['id']}.nc")
    client=cdsapi.Client(url=EWDS_API_URL,key=key,quiet=True)
    req={
      'system_version':['operational'],
      'hydrological_model':['lisflood'],
      'product_type':['control_forecast'],
      'variable':['river_discharge_in_the_last_24_hours'],
      'year':[d.strftime('%Y')], 'month':[d.strftime('%m')], 'day':[d.strftime('%d')],
      'leadtime_hour':[str(h) for h in range(24,min(720,lead_days*24)+1,24)],
      'geographical_area':[station['lat']+.1,station['lon']-.1,station['lat']-.1,station['lon']+.1],
      'data_format':'netcdf4','download_format':'unarchived'
    }
    client.retrieve('cems-glofas-forecast',req,path)
    return path

def replay_case(issue_date,station,observations=None,lead_days=15):
    path=fetch_historical_forecast(issue_date,station,lead_days)
    forecast=extract_glofas_series(path,station['lat'],station['lon'])
    return {'issue_date':issue_date,'station':station['id'],'forecast_discharge':forecast,'ground_truth_source':'BWDB Hydrology observations','ground_truth':observations or [],'leakage_safe':True}
