from flask import Flask, render_template, jsonify, request, Response, session, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import date, datetime, timedelta, timezone
import math, threading, time, statistics, os, sqlite3, re, urllib.parse, json
from real_data import source_status, fetch_glofas_forecast, fetch_station_ensemble, live_snapshot, fetch_historical_forecast

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, 'floodguard.db')
app = Flask(__name__, template_folder='.', static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'floodguard-dev-secret-change-me')

# Operational data source: Copernicus CEMS / GloFAS only. No synthetic fallback.

# Representative GloFAS grid zones. Coordinates identify the nearest GloFAS river-grid cell; they are not Bangladesh gauge stations.
STATIONS = {
 'sylhet': {'name':'Sylhet Region — GloFAS Point','district':'Sylhet','division':'Sylhet','river':'Surma basin','station':'GloFAS grid point','lat':24.895,'lon':91.869},
 'kurigram': {'name':'Kurigram Region — GloFAS Point','district':'Kurigram','division':'Rangpur','river':'Dharla basin','station':'GloFAS grid point','lat':25.805,'lon':89.636},
 'sirajganj': {'name':'Sirajganj Region — GloFAS Point','district':'Sirajganj','division':'Rajshahi','river':'Jamuna basin','station':'GloFAS grid point','lat':24.453,'lon':89.700},
 'rajshahi': {'name':'Rajshahi Region — GloFAS Point','district':'Rajshahi','division':'Rajshahi','river':'Padma basin','station':'GloFAS grid point','lat':24.374,'lon':88.604},
 'faridpur': {'name':'Faridpur Region — GloFAS Point','district':'Faridpur','division':'Dhaka','river':'Padma basin','station':'GloFAS grid point','lat':23.607,'lon':89.842},
 'feni': {'name':'Feni Region — GloFAS Point','district':'Feni','division':'Chattogram','river':'Muhuri basin','station':'GloFAS grid point','lat':23.015,'lon':91.396},
}

# No embedded bulletin/snapshot fallback. Source data must be fetched authentically.

LIVE_CACHE = {'payload': None, 'expires': 0, 'error': None, 'state': 'idle', 'last_attempt': None}
CACHE_SECONDS = int(os.environ.get('LIVE_REFRESH_SECONDS', '300'))
_cache_lock = threading.Lock(); _worker_lock = threading.Lock(); _worker_running = False

def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db()
    con.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, zone TEXT, language TEXT DEFAULT "en", whatsapp TEXT, alerts INTEGER DEFAULT 0, email_alerts INTEGER DEFAULT 1, whatsapp_alerts INTEGER DEFAULT 1)')
    # Backward-compatible columns for existing databases
    cols={r[1] for r in con.execute('PRAGMA table_info(users)').fetchall()}
    if 'email_alerts' not in cols: con.execute('ALTER TABLE users ADD COLUMN email_alerts INTEGER DEFAULT 1')
    if 'whatsapp_alerts' not in cols: con.execute('ALTER TABLE users ADD COLUMN whatsapp_alerts INTEGER DEFAULT 1')
    con.execute('CREATE TABLE IF NOT EXISTS alert_events (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, event_type TEXT NOT NULL, event_key TEXT NOT NULL, sent_at TEXT NOT NULL, UNIQUE(user_id,event_type,event_key))')
    con.execute('CREATE TABLE IF NOT EXISTS alert_state (user_id INTEGER PRIMARY KEY, last_risk TEXT, last_daily_date TEXT, welcome_sent INTEGER DEFAULT 0, updated_at TEXT NOT NULL)')
    con.execute('CREATE TABLE IF NOT EXISTS push_subscriptions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, endpoint TEXT NOT NULL UNIQUE, p256dh TEXT NOT NULL, auth TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)')
    con.commit(); con.close()

init_db()

# ---------- Notification engine ----------
_ALERT_THREAD_STARTED=False
def _smtp_configured():
    # Render Free blocks outbound SMTP ports; use the HTTP webhook path instead.
    return False

def _email_configured(): return bool(os.environ.get('EMAIL_SCRIPT_URL'))
def _whatsapp_bridge_configured(): return False

def _send_email(to_email, subject, body):
    # Backward-compatible Google Apps Script webhook path, if configured.
    webhook=os.environ.get('EMAIL_SCRIPT_URL','').strip()
    if webhook:
        try:
            import requests
            r=requests.post(webhook,json={'to':to_email,'subject':subject,'body':body},timeout=15)
            if 200 <= r.status_code < 300: return True, 'sent via email webhook'
            return False, f'Email webhook HTTP {r.status_code}'
        except Exception as exc: return False, str(exc)
    return False, 'Email backend is not configured. Set EMAIL_SCRIPT_URL.'

def _send_whatsapp(number, body):
    return False, 'WhatsApp delivery is not enabled in the real-data-only build.'

def _send_web_push(user_id, title, body):
    con=db(); rows=con.execute('SELECT endpoint,p256dh,auth FROM push_subscriptions WHERE user_id=?',(user_id,)).fetchall(); con.close()
    if not rows: return False, 'No browser push subscription.'
    try:
        from pywebpush import webpush, WebPushException
        private=os.environ.get('VAPID_PRIVATE_KEY','').strip(); claims=os.environ.get('VAPID_CLAIMS_EMAIL','').strip()
        if not private or not claims: return False, 'VAPID credentials are not configured.'
        sent=False
        for r in rows:
            try:
                webpush(subscription_info={'endpoint':r['endpoint'],'keys':{'p256dh':r['p256dh'],'auth':r['auth']}},data=json.dumps({'title':title,'body':body}),vapid_private_key=private,vapid_claims={'sub':claims})
                sent=True
            except WebPushException as exc:
                if getattr(exc,'response',None) is not None and exc.response.status_code in (404,410):
                    con=db(); con.execute('DELETE FROM push_subscriptions WHERE endpoint=?',(r['endpoint'],)); con.commit(); con.close()
        return sent, 'sent via browser push' if sent else 'browser push failed'
    except Exception as exc: return False, str(exc)

def _user_event_sent(user_id,event_type,event_key):
    con=db(); row=con.execute('SELECT 1 FROM alert_events WHERE user_id=? AND event_type=? AND event_key=?',(user_id,event_type,event_key)).fetchone(); con.close(); return bool(row)

def _mark_event(user_id,event_type,event_key):
    con=db(); con.execute('INSERT OR IGNORE INTO alert_events(user_id,event_type,event_key,sent_at) VALUES(?,?,?,?)',(user_id,event_type,event_key,datetime.now(timezone.utc).isoformat())); con.commit(); con.close()

def _get_alert_state(user_id):
    con=db(); row=con.execute('SELECT * FROM alert_state WHERE user_id=?',(user_id,)).fetchone(); con.close(); return row

def _set_alert_state(user_id, **fields):
    cur=_get_alert_state(user_id)
    now=datetime.now(timezone.utc).isoformat()
    last_risk=fields.get('last_risk', cur['last_risk'] if cur else None)
    last_daily_date=fields.get('last_daily_date', cur['last_daily_date'] if cur else None)
    welcome_sent=fields.get('welcome_sent', cur['welcome_sent'] if cur else 0)
    con=db(); con.execute('INSERT INTO alert_state(user_id,last_risk,last_daily_date,welcome_sent,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET last_risk=excluded.last_risk,last_daily_date=excluded.last_daily_date,welcome_sent=excluded.welcome_sent,updated_at=excluded.updated_at',(user_id,last_risk,last_daily_date,welcome_sent,now)); con.commit(); con.close()

def _message_for(z,lang,kind):
    risk=z.get('risk'); district=z.get('district'); current=z.get('current'); trend=z.get('trend_m3s'); predicted=z.get('predicted')
    if risk=='UNAVAILABLE' or current is None: return f'FloodGuard BD — authentic GloFAS data is still being connected for {district}. No artificial hydrological value is substituted.'
    direction='rising' if (trend or 0)>0 else ('falling' if (trend or 0)<0 else 'stable')
    if lang=='bn':
        if kind=='welcome': return f"FloodGuard BD\nআপনার {district} zone-এর alert চালু হয়েছে। বর্তমান GloFAS relative signal: {risk_label_bn(risk)}।"
        if kind=='daily': return f"FloodGuard BD — দৈনিক আপডেট\n{district}: GloFAS discharge {current:.1f} m³/s; signal {risk_label_bn(risk)}; day-2 trend {direction}।"
        return f"FloodGuard BD সতর্কতা\n{district}-এ GloFAS relative high-flow signal: {risk_label_bn(risk)}। Forecast discharge {current:.1f} m³/s। স্থানীয় কর্তৃপক্ষের নির্দেশনা অনুসরণ করুন।"
    if kind=='welcome': return f"FloodGuard BD\nAlerts for {district} are enabled. Current GloFAS relative signal: {risk}."
    if kind=='daily': return f"FloodGuard BD — Daily update\n{district}: GloFAS discharge {current:.1f} m³/s; signal {risk}; day-2 trend {direction}."
    return f"FloodGuard BD alert\n{district}: GloFAS relative high-flow signal changed to {risk}. Forecast discharge: {current:.1f} m³/s. Follow local-authority guidance."

def risk_label_bn(r): return {'NORMAL':'স্বাভাবিক','WARNING':'সতর্কতা','FLOOD':'বন্যার ঝুঁকি','SEVERE':'তীব্র ঝুঁকি'}.get(r,r)

def _dispatch_user_event(row, event_type, event_key, kind, z):
    if _user_event_sent(row['id'],event_type,event_key): return {'sent':False,'skipped':'already_sent'}
    subject=f"FloodGuard BD — {z['district']} update"
    body=_message_for(z,row['language'] or 'en',kind)
    sent=False; details=[]
    if row['email_alerts'] and row['alerts']:
        ok,detail=_send_email(row['email'],subject,body); details.append('email:'+('sent' if ok else detail)); sent=sent or ok
    if row['alerts']:
        ok,detail=_send_web_push(row['id'],subject,body); details.append('web:'+('sent' if ok else detail)); sent=sent or ok
    if sent: _mark_event(row['id'],event_type,event_key)
    return {'sent':sent,'details':details}

def dispatch_alerts():
    now=datetime.now(timezone.utc); today=now.strftime('%Y-%m-%d')
    con=db(); users=con.execute('SELECT * FROM users WHERE alerts=1').fetchall(); con.close(); results=[]
    for row in users:
        try:
            z=package_station(row['zone'] if row['zone'] in STATIONS else 'sylhet')
            if z.get('risk')=='UNAVAILABLE':
                results.append({'sent':False,'skipped':'authentic_data_unavailable','zone':z.get('id')}); continue
            state=_get_alert_state(row['id'])
            # First run after enabling alerts: welcome + initialize state.
            if not state or not state['welcome_sent']:
                r=_dispatch_user_event(row,'welcome','v1','welcome',z); results.append(r)
                if r.get('sent') or not (row['email_alerts'] or row['whatsapp_alerts']): _set_alert_state(row['id'],welcome_sent=1,last_risk=z['risk'])
            else:
                # Notify only when risk actually changes.
                if state['last_risk'] and state['last_risk'] != z['risk']:
                    r=_dispatch_user_event(row,'risk_change',f"{state['last_risk']}->{z['risk']}",'change',z); results.append(r)
                    if r.get('sent'): _set_alert_state(row['id'],last_risk=z['risk'])
                elif not state['last_risk']:
                    _set_alert_state(row['id'],last_risk=z['risk'])
            state=_get_alert_state(row['id'])
            if not state or state['last_risk'] == z['risk']:
                if not state or state['last_daily_date'] != today:
                    r=_dispatch_user_event(row,'daily',today,'daily',z); results.append(r)
                    if r.get('sent'): _set_alert_state(row['id'],last_daily_date=today,last_risk=z['risk'])
        except Exception as exc: results.append({'sent':False,'error':str(exc)})
    return results

def _alert_loop():
    while True:
        try: dispatch_alerts()
        except Exception: pass
        time.sleep(int(os.environ.get('ALERT_CHECK_SECONDS','300')))

def start_alert_loop():
    global _ALERT_THREAD_STARTED
    if _ALERT_THREAD_STARTED: return
    _ALERT_THREAD_STARTED=True
    threading.Thread(target=_alert_loop,daemon=True,name='floodguard-alerts').start()


GLOFAS_CACHE={'payload':None,'expires':0,'error':None}
CACHE_SECONDS=int(os.environ.get('GLOFAS_CACHE_SECONDS','1800'))

def _glofas_payload(force=False):
    try:
        payload=live_snapshot(STATIONS,force=force)
        GLOFAS_CACHE.update(payload=payload,expires=time.time()+CACHE_SECONDS,error=None)
        return payload
    except Exception as exc:
        GLOFAS_CACHE['error']=str(exc)
        return None

_GLOFAS_THREAD=None
_GLOFAS_FETCHING=False
_GLOFAS_FETCH_STARTED_AT=None
_GLOFAS_FETCH_FINISHED_AT=None

def _start_glofas_fetch(force=False):
    global _GLOFAS_THREAD,_GLOFAS_FETCHING
    if _GLOFAS_FETCHING and _GLOFAS_THREAD and _GLOFAS_THREAD.is_alive():
        return False
    def worker():
        global _GLOFAS_FETCHING, _GLOFAS_FETCH_STARTED_AT, _GLOFAS_FETCH_FINISHED_AT
        _GLOFAS_FETCHING=True
        _GLOFAS_FETCH_STARTED_AT=time.time()
        _GLOFAS_FETCH_FINISHED_AT=None
        try:
            payload=_glofas_payload(force=force)
            if payload and payload.get('stations'):
                try: dispatch_alerts()
                except Exception: pass
        finally:
            _GLOFAS_FETCHING=False
            _GLOFAS_FETCH_FINISHED_AT=time.time()
    _GLOFAS_THREAD=threading.Thread(target=worker,daemon=True,name='glofas-refresh')
    _GLOFAS_THREAD.start()
    return True

def _series(key, force=False):
    # Request handlers must NEVER perform a GloFAS download. The background
    # refresh owns network I/O; pages read the latest authentic snapshot only.
    payload=GLOFAS_CACHE.get('payload')
    if not payload or not payload.get('stations',{}).get(key): return []
    return payload['stations'][key]

def risk_for_discharge(series, value):
    """Return a relative *forecast-window position*, never a flood probability.

    GloFAS supplies discharge forecasts, not Bangladesh warning-stage thresholds
    in this app. Therefore the UI must not call this number a probability.
    """
    if value is None or not series:return 'UNAVAILABLE',None
    vals=sorted(float(x['discharge_m3s']) for x in series if x.get('discharge_m3s') is not None)
    if not vals:return 'UNAVAILABLE',None
    if len(vals)==1:return 'NORMAL',None
    rank=100.0*(sum(1 for x in vals if x < float(value)) + 0.5*sum(1 for x in vals if x == float(value)))/len(vals)
    if rank>=97.5:r='SEVERE'
    elif rank>=90:r='FLOOD'
    elif rank>=75:r='WARNING'
    else:r='NORMAL'
    return r,round(rank,1)

def package_station(key, force=False):
    st=STATIONS[key]; series=_series(key,force=force); first=series[0] if series else None
    current=first.get('discharge_m3s') if first else None
    risk,prob=risk_for_discharge(series,current)
    trend=None
    if len(series)>=2: trend=round(float(series[1]['discharge_m3s'])-float(series[0]['discharge_m3s']),2)
    forecast=[]
    for x in series[:15]:
        rr,pp=risk_for_discharge(series,x.get('discharge_m3s'))
        forecast.append({'date':((datetime.fromisoformat((GLOFAS_CACHE.get('payload') or {}).get('issue_date')) if (GLOFAS_CACHE.get('payload') or {}).get('issue_date') else datetime.now(timezone.utc)) + timedelta(days=int(x['lead_day']))).strftime('%Y-%m-%d'),'level':round(float(x['discharge_m3s']),1),'discharge_m3s':round(float(x['discharge_m3s']),1),'probability':pp,'risk':rr,'uncertainty':round((float(x['p90_m3s'])-float(x['p10_m3s']))/2,1) if x.get('p90_m3s') is not None else None,'source':'GloFAS operational forecast'})
    status=('CACHED GLOFAS' if (GLOFAS_CACHE.get('payload') or {}).get('stale') else 'LIVE GLOFAS') if current is not None else 'CONNECTING TO GLOFAS'
    return {'id':key,'name':st['name'],'district':st['district'],'division':st['division'],'river':st['river'],'station':st['station'],
            'current':round(float(current),1) if current is not None else None,'predicted':forecast[0]['level'] if forecast else None,'probability':None,'signal_position':prob,
            'risk':risk,'danger':None,'reference':None,'lat':st['lat'],'lon':st['lon'],'trend_3h_cm':trend,'trend_m3s':trend,
            'live':current is not None,'simulation':False,'snapshot':False,'status':status,'source':'Copernicus CEMS / GloFAS','source_url':'https://ewds.climate.copernicus.eu/datasets/cems-glofas-forecast',
            'observed_at':None,'fetched_at':(GLOFAS_CACHE.get('payload') or {}).get('fetched_at'),'issue_date':(GLOFAS_CACHE.get('payload') or {}).get('issue_date'),'stale':bool((GLOFAS_CACHE.get('payload') or {}).get('stale')),'forecast15':forecast,'day15':forecast[-1]['level'] if forecast else None,'day15risk':forecast[-1]['risk'] if forecast else 'UNAVAILABLE',
            'history_points':0,'model_ready':True,'unit':'m3/s',"signal_type":"Relative position within this issue date's GloFAS forecast window; not flood probability"}

def build_dashboard(key):
    if key not in STATIONS:key='sylhet'
    z=package_station(key)
    return {'station':z,'current':z['current'],'predicted':z['predicted'],'probability':z['probability'],'risk':z['risk'],'trend_per_3h':z['trend_m3s'],'forecast15':z['forecast15'],'history':[],'advice':advice(z['risk']),'simple':make_simple_summary(z),'live_connected':z['live'],'model':{'ready':True,'model_name':'Copernicus GloFAS / LISFLOOD operational ensemble'}}

def advice(risk):
    if risk=='UNAVAILABLE': return ['GloFAS forecast data is currently unavailable for this zone.','FloodGuard will not substitute simulated values.','Follow official Bangladesh flood authorities for decisions.']
    return {'NORMAL':['Monitor the GloFAS outlook and local conditions.','Keep phones and power banks charged.','Know the nearest safe high ground or shelter.'],'WARNING':['Check the next GloFAS updates and local-authority information regularly.','Prepare water, dry food, medicines and important documents.','Plan an evacuation route if local conditions worsen.'],'FLOOD':['Treat the high-flow signal seriously and monitor official warnings.','Move valuables, documents, livestock and essentials higher.','Avoid unnecessary travel near rivers and fast-moving water.'],'SEVERE':['Treat the forecast as a strong high-flow signal and check official warnings immediately.','Follow local-authority emergency instructions.','Move to higher ground or a designated shelter when instructed.']}[risk]

def make_simple_summary(z):
    if z['current'] is None: return {'headline':f"Connecting to authentic GloFAS for {z['district']}.",'sub':'FloodGuard never substitutes a synthetic hydrological value.'}
    direction='rising' if (z['trend_m3s'] or 0)>0 else ('falling' if (z['trend_m3s'] or 0)<0 else 'steady')
    if z['risk']=='SEVERE': headline=f"Very high relative forecast signal near {z['district']}. The GloFAS forecast is {direction}."
    elif z['risk']=='FLOOD': headline=f"High relative forecast signal near {z['district']}. The GloFAS forecast is {direction}."
    elif z['risk']=='WARNING': headline=f"Forecast signal is elevated near {z['district']}. The GloFAS forecast is {direction}."
    else: headline=f"No elevated relative forecast signal is detected near {z['district']} in the current GloFAS forecast."
    return {'headline':headline,'sub':f"Forecast discharge: {z['current']:.1f} m³/s. This is a GloFAS forecast discharge signal, not an observed Bangladesh gauge stage."}

@app.get('/healthz')
def healthz():
    payload=GLOFAS_CACHE.get('payload')
    return jsonify({'ok':True,'service':'FloodGuard BD','glofas_connected':bool(payload),'glofas_issue_date':payload.get('issue_date') if payload else None,'glofas_fetched_at':payload.get('fetched_at') if payload else None,'background_alerts':bool(_ALERT_THREAD_STARTED)})

PAGE_ROUTES = {
    '/': 'dashboard',
    '/monitor': 'zones',
    '/zones': 'zones',
    '/map': 'maps',
    '/forecast': 'forecast',
    '/analytics': 'analytics',
    '/research': 'research',
    '/how-it-works': 'about',
    '/hazards': 'hazards',
    '/simulation': 'simulation',
    '/alerts': 'account',
}

def _serve_app_page(page='dashboard'):
    root_index = os.path.join(BASE, 'index.html')
    if os.path.exists(root_index):
        response = send_file(root_index)
        response.headers['X-FloodGuard-Page'] = page
        return response
    return render_template('index.html')

@app.route('/')
def index():
    return _serve_app_page('dashboard')

for _route, _page in PAGE_ROUTES.items():
    if _route == '/':
        continue
    app.add_url_rule(_route, endpoint='page_' + _page + _route.replace('/', '_').replace('-', '_'), view_func=lambda page=_page: _serve_app_page(page), methods=['GET'])
@app.route('/api/zones')
def zones(): return jsonify([package_station(k) for k in STATIONS])
@app.route('/api/dashboard')
def dashboard(): return jsonify(build_dashboard(request.args.get('station','sylhet')))
@app.route('/api/national')
def national():
    zones=[package_station(k) for k in STATIONS]; counts={r:sum(z['risk']==r for z in zones) for r in ['NORMAL','WARNING','FLOOD','SEVERE']}
    return jsonify({'zones':zones,'counts':counts,'stations':len(zones),'live_stations':sum(z['live'] for z in zones)})
@app.route('/api/live-refresh')
def live_refresh():
    started=_start_glofas_fetch(force=request.args.get('force')=='1')
    snap=GLOFAS_CACHE.get('payload')
    return jsonify({'started':started,'state':'fetching' if _GLOFAS_FETCHING else ('ready' if snap else 'idle'),'connected':bool(snap),'fetched_at':snap.get('fetched_at') if snap else None,'error':GLOFAS_CACHE.get('error')})
@app.route('/api/live-status')
def live_status():
    snap=GLOFAS_CACHE.get('payload'); return jsonify({'connected':bool(snap),'state':'fetching' if _GLOFAS_FETCHING else ('ready' if snap else ('error' if GLOFAS_CACHE.get('error') else 'idle')),'source':'Copernicus CEMS / GloFAS','fetched_at':snap.get('fetched_at') if snap else None,'issue_date':snap.get('issue_date') if snap else None,'error':GLOFAS_CACHE.get('error')})
@app.get('/api/glofas/diagnostics')
def glofas_diagnostics():
    snap=GLOFAS_CACHE.get('payload') or {}
    stations=snap.get('stations') or {}
    lengths={k:len(v or []) for k,v in stations.items()}
    elapsed=None
    if _GLOFAS_FETCH_STARTED_AT:
        elapsed=(time.time()-_GLOFAS_FETCH_STARTED_AT) if _GLOFAS_FETCHING else max(0.0, (_GLOFAS_FETCH_FINISHED_AT or time.time())-_GLOFAS_FETCH_STARTED_AT)
    return jsonify({'connected':bool(snap),'fetching':bool(_GLOFAS_FETCHING),'issue_date':snap.get('issue_date'),'fetched_at':snap.get('fetched_at'),'stale':bool(snap.get('stale')),'station_count':len(stations),'forecast_lengths':lengths,'min_forecast_days':min(lengths.values()) if lengths else 0,'max_forecast_days':max(lengths.values()) if lengths else 0,'last_error':GLOFAS_CACHE.get('error') or snap.get('last_error'),'preflight':None,'fetch_elapsed_seconds':round(elapsed,1) if elapsed is not None else None,'request_timeout_seconds':int(os.environ.get('GLOFAS_REQUEST_TIMEOUT_SECONDS','150')),'source':'Copernicus CEMS / GloFAS'})
@app.route('/api/analytics')
def analytics():
    zones=[package_station(k) for k in STATIONS]; valid=[z for z in zones if z.get('current') is not None]
    rising=sorted(valid,key=lambda z:z.get('trend_m3s') if z.get('trend_m3s') is not None else -999,reverse=True)
    return jsonify({'counts':{r:sum(z['risk']==r for z in zones) for r in ['NORMAL','WARNING','FLOOD','SEVERE']},'avg_discharge_m3s':round(statistics.mean(z['current'] for z in valid),1) if valid else None,'rising':rising[:6],'highest_signal':sorted(valid,key=lambda z:z.get('signal_position') or 0,reverse=True)[:6]})

@app.get('/api/model-status')
def api_model_status(): return jsonify({'ready':True,'model_name':'Copernicus GloFAS / LISFLOOD operational ensemble','training_source':'ECMWF meteorological ensemble + LISFLOOD hydrological model','synthetic_data_used':False,'note':'FloodGuard uses GloFAS discharge forecasts. Relative signal bands are descriptive within the selected forecast window and are not flood probabilities or official Bangladesh warning thresholds.'})

@app.get('/api/data-provenance')
def data_provenance(): return jsonify(source_status())

@app.get('/api/research/summary')
def research_summary():
    zones=[package_station(k) for k in STATIONS]
    return jsonify({'ok':True,'architecture':'General Mode + Research Mode · GloFAS-only','source':source_status(),'zones':len(zones),'live_zones':sum(z['live'] for z in zones),"risk_definition":"Relative position within the selected issue date's GloFAS forecast window. It is descriptive, not a flood probability and not an official Bangladesh warning threshold.",'forecast_horizon_days':15,'ensemble_enabled':any(v.get('state')=='ready' for k,v in _RESEARCH_JOBS.items() if k.startswith('ensemble:')),'historical_replay':'AVAILABLE_ON_DEMAND','verification':'GloFAS forecast vs GloFAS historical modelled discharge; not independent gauge validation','synthetic_fallback':False})


# Research-mode jobs are asynchronous so a slow EWDS queue never blocks a Render Free HTTP worker.
_RESEARCH_JOBS = {}
_RESEARCH_JOBS_LOCK = threading.RLock()

def _start_research_job(job_id, fn):
    with _RESEARCH_JOBS_LOCK:
        existing=_RESEARCH_JOBS.get(job_id)
        if existing and existing.get('state') == 'running':
            return False
        _RESEARCH_JOBS[job_id]={'state':'running','started_at':time.time(),'result':None,'error':None}
    def worker():
        try:
            result=fn()
            with _RESEARCH_JOBS_LOCK:
                _RESEARCH_JOBS[job_id].update(state='ready',result=result,finished_at=time.time())
        except Exception as exc:
            with _RESEARCH_JOBS_LOCK:
                _RESEARCH_JOBS[job_id].update(state='error',error=str(exc),finished_at=time.time())
    threading.Thread(target=worker,daemon=True,name='floodguard-research').start()
    return True

def _research_job_response(job_id):
    with _RESEARCH_JOBS_LOCK:
        job=_RESEARCH_JOBS.get(job_id)
        if not job: return {'ok':False,'state':'missing','job_id':job_id}
        out={'ok':job.get('state')=='ready','state':job.get('state'),'job_id':job_id}
        if job.get('result') is not None: out.update(job['result'] if isinstance(job['result'],dict) else {'result':job['result']})
        if job.get('error'): out['error']=job['error']
        return out

@app.get('/api/research/glofas/<station_id>')
def research_glofas(station_id):
    if station_id not in STATIONS:return jsonify({'ok':False,'error':'Unknown station'}),404
    force=request.args.get('force')=='1'
    job_id=f"ensemble:{station_id}"
    with _RESEARCH_JOBS_LOCK:
        cached=_RESEARCH_JOBS.get(job_id)
        if cached and cached.get('state')=='ready' and not force:
            return jsonify(_research_job_response(job_id))
        if cached and cached.get('state')=='running':
            return jsonify(_research_job_response(job_id))
    _start_research_job(job_id, lambda: fetch_station_ensemble(STATIONS[station_id],force=force))
    return jsonify({'ok':False,'state':'fetching','job_id':job_id,'station':station_id,'source':'Copernicus CEMS / GloFAS operational ensemble'})

@app.get('/api/research/job/<path:job_id>')
def research_job(job_id):
    return jsonify(_research_job_response(job_id))

def _verification_result(station_id, issue_date):
    from real_data import fetch_historical_series
    replay=fetch_historical_forecast(STATIONS[station_id],issue_date,lead_days=15); fc=replay.get('forecast',[])
    issue=datetime.fromisoformat(issue_date).date(); valid_days=[issue+timedelta(days=i) for i in range(1,len(fc)+1)]
    hist=fetch_historical_series(STATIONS[station_id],valid_days); by_date={r['date']:r['discharge_m3s'] for r in hist}; pairs=[]
    for row in fc:
        d=(issue+timedelta(days=int(row['lead_day']))).isoformat()
        if d in by_date and row.get('discharge_m3s') is not None:pairs.append((int(row['lead_day']),float(row['discharge_m3s']),float(by_date[d]),d))
    if not pairs: raise RuntimeError('Historical GloFAS target values were not returned for the replay window.')
    errors=[a-b for _,a,b,_ in pairs]; mae=sum(abs(x) for x in errors)/len(errors); rmse=(sum(x*x for x in errors)/len(errors))**0.5; bias=sum(errors)/len(errors)
    ma=sum(a for _,a,_,_ in pairs)/len(pairs); mb=sum(b for _,_,b,_ in pairs)/len(pairs); cov=sum((a-ma)*(b-mb) for _,a,b,_ in pairs); va=sum((a-ma)**2 for _,a,_,_ in pairs); vb=sum((b-mb)**2 for _,_,b,_ in pairs); corr=cov/(va*vb)**0.5 if va>0 and vb>0 else None
    return {'ok':True,'station':station_id,'issue_date':issue_date,'target':'GloFAS v4.0 historical modelled discharge (intermediate)' ,'independent_gauge_validation':False,'n':len(pairs),'mae_m3s':round(mae,2),'rmse_m3s':round(rmse,2),'bias_m3s':round(bias,2),'correlation':round(corr,3) if corr is not None else None,'rows':[{'lead_day':d,'date':dt,'forecast_m3s':round(a,2),'historical_m3s':round(b,2),'error_m3s':round(a-b,2)} for d,a,b,dt in pairs]}

@app.post('/api/research/replay')
def research_replay():
    data=request.get_json(silent=True) or {}; station_id=data.get('station_id','feni'); issue_date=(data.get('issue_date') or '').strip()
    if station_id not in STATIONS or not issue_date:return jsonify({'ok':False,'error':'station_id and issue_date are required'}),400
    try: parsed_issue=datetime.fromisoformat(issue_date).date()
    except Exception:return jsonify({'ok':False,'error':'issue_date must be YYYY-MM-DD'}),400
    if parsed_issue < date(2019,11,5) or parsed_issue > datetime.now(timezone.utc).date():
        return jsonify({'ok':False,'error':'Operational GloFAS forecast replay supports issue dates from 2019-11-05 through today (UTC).'}),400
    job_id=f"replay:{station_id}:{issue_date}"
    with _RESEARCH_JOBS_LOCK:
        cached=_RESEARCH_JOBS.get(job_id)
        if cached and cached.get('state')=='ready': return jsonify(_research_job_response(job_id))
        if cached and cached.get('state')=='running': return jsonify(_research_job_response(job_id))
    _start_research_job(job_id, lambda: fetch_historical_forecast(STATIONS[station_id],issue_date,lead_days=15))
    return jsonify({'ok':False,'state':'fetching','job_id':job_id,'station':station_id,'issue_date':issue_date})

@app.post('/api/research/verify')
def research_verify():
    data=request.get_json(silent=True) or {}; station_id=data.get('station_id','feni'); issue_date=(data.get('issue_date') or '').strip()
    if station_id not in STATIONS or not issue_date:return jsonify({'ok':False,'error':'station_id and issue_date are required'}),400
    try: parsed_issue=datetime.fromisoformat(issue_date).date()
    except Exception:return jsonify({'ok':False,'error':'issue_date must be YYYY-MM-DD'}),400
    if parsed_issue < date(2019,11,5) or parsed_issue > datetime.now(timezone.utc).date():
        return jsonify({'ok':False,'error':'Operational GloFAS forecast verification supports issue dates from 2019-11-05 through today (UTC).'}),400
    job_id=f"verify:{station_id}:{issue_date}"
    with _RESEARCH_JOBS_LOCK:
        cached=_RESEARCH_JOBS.get(job_id)
        if cached and cached.get('state')=='ready': return jsonify(_research_job_response(job_id))
        if cached and cached.get('state')=='running': return jsonify(_research_job_response(job_id))
    _start_research_job(job_id, lambda: _verification_result(station_id,issue_date))
    return jsonify({'ok':False,'state':'fetching','job_id':job_id,'station':station_id,'issue_date':issue_date})

@app.get('/api/research/validation-status')
def validation_status():
    return jsonify({'status':'READY_FOR_MODELLED-TARGET VERIFICATION','synthetic_data_used':False,'real_observation_validation':'NOT_AVAILABLE_WITH_GLOFAS-ONLY INPUTS','available_verification':'Operational GloFAS forecast replay vs GloFAS v4.0 historical modelled discharge (intermediate)' ,'independent_ground_truth':False,'note':'This verification measures consistency against a GloFAS historical modelled target; it must not be presented as gauge-based flood-warning accuracy.'})

@app.get('/api/glofas/<station_id>')
def glofas_station(station_id):
    if station_id not in STATIONS:return jsonify({'ok':False,'error':'Unknown station'}),404
    try:return jsonify(fetch_glofas_forecast(STATIONS,station_id,force=request.args.get('force')=='1'))
    except Exception as exc:return jsonify({'ok':False,'error':str(exc),'source':'Copernicus GloFAS'}),503

@app.get('/api/hazards/earthquakes')
def earthquakes():
    """Recent earthquake monitoring near Bangladesh using the public USGS GeoJSON feed.
    Monitoring only; no earthquake prediction is performed.
    """
    import math
    try:
        import requests
        from datetime import timedelta
        days=max(1,min(int(request.args.get('days','7')),30))
        minmag=float(request.args.get('minmagnitude','2.5'))
        end=datetime.now(timezone.utc)
        start=end-timedelta(days=days)
        url='https://earthquake.usgs.gov/fdsnws/event/1/query'
        params={'format':'geojson','starttime':start.isoformat(),'endtime':end.isoformat(),'minmagnitude':minmag,'latitude':23.685,'longitude':90.356,'maxradiuskm':900,'limit':200,'orderby':'time'}
        r=requests.get(url,params=params,timeout=12,headers={'User-Agent':'FloodGuard-BD/1.0'})
        r.raise_for_status(); data=r.json()
        def hav(lat1,lon1,lat2,lon2):
            R=6371.0; p1=math.radians(lat1); p2=math.radians(lat2); dp=math.radians(lat2-lat1); dl=math.radians(lon2-lon1)
            h=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
            return R*2*math.atan2(math.sqrt(h),math.sqrt(max(0,1-h)))
        ev=[]
        for f in data.get('features',[]):
            p=f.get('properties') or {}; coords=(f.get('geometry') or {}).get('coordinates') or []
            if len(coords)<3: continue
            lon,lat,depth=coords[0],coords[1],coords[2]
            mag=p.get('mag')
            if lat is None or lon is None or mag is None: continue
            dist=hav(23.685,90.356,float(lat),float(lon))
            ts=p.get('time')
            try: local=datetime.fromtimestamp(ts/1000,tz=timezone.utc).astimezone().strftime('%d %b %Y, %I:%M %p')
            except Exception: local='Unknown time'
            ev.append({'id':f.get('id'),'place':p.get('place') or 'Unknown location','magnitude':float(mag),'depth_km':float(depth or 0),'distance_km':float(dist),'local_time':local,'time_ms':ts,'url':p.get('url')})
        ev.sort(key=lambda x:x.get('time_ms') or 0,reverse=True)
        return jsonify({'ok':True,'count':len(ev),'max_magnitude':max((x['magnitude'] for x in ev),default=None),'nearest_km':min((x['distance_km'] for x in ev),default=None),'center':{'lat':23.685,'lon':90.356},'source':'USGS','source_url':'https://earthquake.usgs.gov/earthquakes/feed/','events':ev})
    except Exception as e:
        return jsonify({'ok':False,'error':str(e),'source':'USGS','source_url':'https://earthquake.usgs.gov/earthquakes/feed/','events':[]}),502



# ---------- Multi-hazard data ----------
WEATHER_CACHE = {}
WEATHER_CACHE_SECONDS = 180

def _weather_cache_get(key):
    item = WEATHER_CACHE.get(key)
    if item and time.time() - item.get('ts', 0) < WEATHER_CACHE_SECONDS:
        return item.get('data')
    return None

def _weather_cache_set(key, data):
    WEATHER_CACHE[key] = {'ts': time.time(), 'data': data}
    return data

def _wmo_label(code):
    labels = {0:'Clear',1:'Mainly clear',2:'Partly cloudy',3:'Overcast',45:'Fog',48:'Depositing rime fog',
              51:'Light drizzle',53:'Drizzle',55:'Dense drizzle',56:'Freezing drizzle',57:'Freezing drizzle',
              61:'Light rain',63:'Rain',65:'Heavy rain',66:'Freezing rain',67:'Heavy freezing rain',
              71:'Light snow',73:'Snow',75:'Heavy snow',77:'Snow grains',80:'Rain showers',81:'Rain showers',
              82:'Heavy rain showers',85:'Snow showers',86:'Heavy snow showers',95:'Thunderstorm',96:'Thunderstorm with hail',99:'Thunderstorm with heavy hail'}
    return labels.get(int(code),'Weather') if code is not None else 'Weather unavailable'

def _weather_hazard_score(current, daily, hourly):
    max_wind = max(daily.get('wind_gusts_10m_max') or [0]) if daily.get('wind_gusts_10m_max') else 0
    max_rain = max(daily.get('precipitation_probability_max') or [0]) if daily.get('precipitation_probability_max') else 0
    max_temp = max(daily.get('temperature_2m_max') or [0]) if daily.get('temperature_2m_max') else 0
    thunder = sum(1 for c in (hourly.get('weather_code') or []) if c is not None and int(c) >= 95)
    rain_sum = max(daily.get('precipitation_sum') or [0]) if daily.get('precipitation_sum') else 0
    severe = []
    if max_wind >= 55: severe.append(('Wind alert','Strong gust potential'))
    elif max_wind >= 40: severe.append(('Wind watch','Gusty conditions possible'))
    if max_rain >= 80 or rain_sum >= 50: severe.append(('Heavy rain watch','High rainfall potential'))
    elif max_rain >= 60 or rain_sum >= 25: severe.append(('Rain watch','Rainfall potential elevated'))
    if thunder >= 1: severe.append(('Thunderstorm watch',f'{thunder} forecast thunderstorm hour(s) in the next 5 days'))
    if max_temp >= 38: severe.append(('Heat watch','High temperature outlook'))
    elif max_temp >= 36: severe.append(('Heat watch','Hot conditions possible'))
    return {'wind_gust_max_kmh':max_wind,'rain_probability_max_pct':max_rain,'rain_sum_max_mm':rain_sum,'thunderstorm_hours':thunder,'max_temp_c':max_temp,'flags':severe}

@app.get('/api/hazards/weather')
def weather():
    try:
        zone_key = request.args.get('station','sylhet')
        z = STATIONS.get(zone_key, STATIONS['sylhet'])
        lat, lon = z['lat'], z['lon']
        cache_key = f'{zone_key}:{round(float(lat),3)}:{round(float(lon),3)}'
        cached = _weather_cache_get(cache_key)
        if cached: return jsonify(cached)
        import requests
        params = {
            'latitude':lat,'longitude':lon,'current':'temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m,wind_gusts_10m,pressure_msl',
            'hourly':'temperature_2m,precipitation_probability,precipitation,weather_code,wind_speed_10m,wind_gusts_10m',
            'daily':'weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,wind_speed_10m_max,wind_gusts_10m_max',
            'forecast_days':5,'timezone':'Asia/Dhaka'
        }
        r=requests.get('https://api.open-meteo.com/v1/forecast',params=params,timeout=8,headers={'User-Agent':'FloodGuard-BD/1.0'}); r.raise_for_status(); d=r.json()
        out={'ok':True,'zone':z['district'],'coordinates':{'lat':lat,'lon':lon},'current':d.get('current',{}),'daily':d.get('daily',{}),'hourly':d.get('hourly',{}),'hazard':_weather_hazard_score(d.get('current',{}),d.get('daily',{}),d.get('hourly',{})),'source':'Open-Meteo'}
        out['current']['label']=_wmo_label(out['current'].get('weather_code'))
        return jsonify(_weather_cache_set(cache_key,out))
    except Exception as e:
        return jsonify({'ok':False,'error':str(e),'source':'Open-Meteo'}),502

@app.get('/api/hazards/cyclones')
def cyclones():
    """In-site tropical-cyclone watch using GDACS API. Does not predict cyclones."""
    try:
        import requests, math, xml.etree.ElementTree as ET
        from datetime import timedelta
        end=datetime.now(timezone.utc); start=end-timedelta(days=14)
        url='https://www.gdacs.org/gdacsapi/api/Events/geteventlist/SEARCH'
        params={'eventlist':'TC','fromdate':start.strftime('%Y-%m-%d'),'todate':end.strftime('%Y-%m-%d'),'alertlevel':'green;orange;red'}
        r=requests.get(url,params=params,timeout=10,headers={'User-Agent':'FloodGuard-BD/1.0'}); r.raise_for_status()
        raw=r.text
        events=[]
        try:
            payload=r.json()
            if isinstance(payload,dict):
                raw_items=payload.get('features') or payload.get('events') or payload.get('data') or []
                for it in raw_items:
                    pr=it.get('properties',it) if isinstance(it,dict) else {}
                    geom=(it.get('geometry') or {}).get('coordinates') if isinstance(it,dict) else None
                    ev={'name':pr.get('eventname') or pr.get('name') or pr.get('eventName') or 'Tropical cyclone', 'alert':str(pr.get('alertlevel') or pr.get('alertLevel') or 'information').upper(), 'lat':None,'lon':None,'wind_kmh':pr.get('maxwind') or pr.get('maxWind') or pr.get('windSpeed')}
                    if geom and len(geom)>=2: ev.update({'lon':float(geom[0]),'lat':float(geom[1])})
                    if ev['lat'] is not None and ev['lon'] is not None: events.append(ev)
        except Exception:
            pass
        # A resilient XML fallback for common GDACS feed structures.
        if not events:
            try:
                root=ET.fromstring(raw)
                for item in root.iter():
                    tag=item.tag.lower()
                    if tag.endswith('item'):
                        txt={c.tag.lower().split('}')[-1]: (c.text or '').strip() for c in item}
                        name=txt.get('eventname') or txt.get('title') or txt.get('name')
                        if name: events.append({'name':name,'alert':(txt.get('alertlevel') or 'information').upper(),'lat':None,'lon':None,'wind_kmh':None})
            except Exception:
                pass
        # Keep the UI useful even when GDACS returns no structured event records.
        events=events[:10]
        return jsonify({'ok':True,'active_count':len(events),'events':events,'source':'GDACS'})
    except Exception as e:
        return jsonify({'ok':False,'error':str(e),'source':'GDACS','active_count':0,'events':[]}),502

@app.route('/api/report')
def report():
    zones=[package_station(k) for k in STATIONS]; now=datetime.now().astimezone().strftime('%d %b %Y, %I:%M:%S %p')
    lines=['FLOODGUARD BD — FLOOD SITUATION REPORT','',f'Generated: {now}','Only authentic Copernicus GloFAS forecast data and derived relative high-flow signals are reported.','']
    for z in zones:
        value='—' if z['current'] is None else f"{z['current']:.1f}"
        lines.append(f"{z['name']} | {value} m3/s | forecast signal {z['risk']} | {z['status']}")
    return Response('\n'.join(lines),mimetype='text/plain',headers={'Content-Disposition':'attachment; filename="FloodGuard_BD_Situation_Report.txt"'})

@app.post('/api/copilot')
def copilot():
    data=request.get_json(silent=True) or {}
    question=(data.get('question') or '').strip()
    station=data.get('station') or 'sylhet'
    if not question: return jsonify({'ok':False,'error':'Question is required.'}),400
    if station not in STATIONS: station='sylhet'
    key=os.environ.get('GEMINI_API_KEY','').strip()
    z=package_station(station)
    if not z.get('live'):
        return jsonify({'ok':True,"answer":"Authentic Copernicus GloFAS data is not loaded for this zone yet, so I will not invent a discharge value or forecast. Once the feed connects, Copilot can answer using the selected zone's real GloFAS forecast context.",'model':'FloodGuard data-gated explainer','grounded_in':'No hydrological data loaded'}),200
    if not key:
        direction='rising' if (z.get('trend_m3s') or 0)>0 else ('falling' if (z.get('trend_m3s') or 0)<0 else 'stable')
        f3=(z.get('forecast15') or [])[2] if len(z.get('forecast15') or [])>=3 else None
        answer=(f"GloFAS forecast discharge for {z['district']} is {z['current']:.1f} m³/s. The Day-2 change is {(z.get('trend_m3s') or 0):+.1f} m³/s, so the forecast is {direction}. FloodGuard classifies the loaded forecast-window position as {z['risk']}; this is not a flood probability. " + (f"Around Day 3 the median forecast is {f3['level']:.1f} m³/s. " if f3 else '') + "This is forecast discharge, not observed Bangladesh gauge stage. Follow official local warnings for emergency decisions.")
        return jsonify({'ok':True,'answer':answer,'model':'FloodGuard grounded explainer','grounded_in':'Copernicus GloFAS data'})
    prompt=("You are FloodGuard BD Copilot. Use ONLY the supplied FloodGuard data. "
            "Do not invent measurements, forecasts, warnings, authorities or sources. "
            "Clearly distinguish GloFAS forecast discharge from any derived relative signal. "
            "Give concise, safety-conscious decision support and tell the user to follow official Bangladesh emergency authorities. Do not call the GloFAS relative signal an official warning.\n\n"
            f"Station data: {json.dumps(z,ensure_ascii=False)}\nUser question: {question}")
    try:
        import requests
        url='https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent'
        r=requests.post(url,params={'key':key},json={'contents':[{'parts':[{'text':prompt}]}]},timeout=20)
        r.raise_for_status(); j=r.json()
        text=((j.get('candidates') or [{}])[0].get('content') or {}).get('parts') or []
        answer=''.join(p.get('text','') for p in text).strip()
        if not answer: raise RuntimeError('Gemini returned an empty response')
        return jsonify({'ok':True,'answer':answer,'model':'gemini-2.5-flash','grounded_in':'FloodGuard real source data'})
    except Exception as exc:
        # The product remains useful when Gemini is unavailable. Never let an
        # optional AI provider break the grounded flood information path.
        direction='rising' if (z.get('trend_m3s') or 0)>0 else ('falling' if (z.get('trend_m3s') or 0)<0 else 'stable')
        f3=(z.get('forecast15') or [])[2] if len(z.get('forecast15') or [])>=3 else None
        answer=(f"Gemini is temporarily unavailable, so FloodGuard is using its grounded fallback. GloFAS forecast discharge for {z['district']} is {z['current']:.1f} m³/s and the Day-2 trend is {(z.get('trend_m3s') or 0):+.1f} m³/s ({direction}). The relative forecast-window classification is {z['risk']}; this is not a flood probability. " + (f"The Day-3 forecast is {f3['level']:.1f} m³/s. " if f3 else '') + "This is GloFAS forecast discharge, not an observed Bangladesh gauge stage. Follow official local warnings for emergency decisions.")
        return jsonify({'ok':True,'answer':answer,'model':'FloodGuard grounded fallback','grounded_in':'Copernicus GloFAS data','provider_error':str(exc)})

@app.get('/api/alerts/config')
def alerts_config():
    return jsonify({'ok':True,'email_configured':_email_configured(),'web_push_configured':bool(os.environ.get('VAPID_PUBLIC_KEY','').strip() and os.environ.get('VAPID_PRIVATE_KEY','').strip() and os.environ.get('VAPID_CLAIMS_EMAIL','').strip()),'whatsapp_configured':False,'sms_configured':False})

@app.get('/api/push/public-key')
def push_public_key():
    key=os.environ.get('VAPID_PUBLIC_KEY','').strip()
    if not key:return jsonify({'ok':False,'error':'VAPID_PUBLIC_KEY is not configured.'}),503
    return jsonify({'ok':True,'public_key':key})

@app.post('/api/push/subscribe')
def push_subscribe():
    uid=session.get('uid')
    if not uid:return jsonify({'ok':False,'error':'Login required.'}),401
    data=request.get_json(silent=True) or {}; sub=data.get('subscription') or {}
    endpoint=sub.get('endpoint'); keys=sub.get('keys') or {}; p256dh=keys.get('p256dh'); auth=keys.get('auth')
    if not endpoint or not p256dh or not auth:return jsonify({'ok':False,'error':'Invalid browser push subscription.'}),400
    now=datetime.now(timezone.utc).isoformat(); con=db(); con.execute('INSERT INTO push_subscriptions(user_id,endpoint,p256dh,auth,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(endpoint) DO UPDATE SET user_id=excluded.user_id,p256dh=excluded.p256dh,auth=excluded.auth,updated_at=excluded.updated_at',(uid,endpoint,p256dh,auth,now,now)); con.commit(); con.close()
    return jsonify({'ok':True})

# --- Auth + WhatsApp notification settings. Actual delivery requires provider credentials. ---
@app.post('/api/auth/register')
def register():
    data=request.get_json(force=True); email=(data.get('email') or '').strip().lower(); password=data.get('password') or ''
    if not email or len(password)<6:return jsonify({'ok':False,'error':'Use a valid email and a password of at least 6 characters.'}),400
    try:
        con=db(); cur=con.execute('INSERT INTO users(email,password_hash,zone,language,whatsapp,alerts,email_alerts,whatsapp_alerts) VALUES(?,?,?,?,?,0,1,1)',(email,generate_password_hash(password),'sylhet','en','')); con.commit(); uid=cur.lastrowid; con.close(); session['uid']=uid
        return jsonify({'ok':True,'user':{'email':email,'zone':'sylhet','language':'en','whatsapp':'','alerts':False}})
    except sqlite3.IntegrityError:return jsonify({'ok':False,'error':'Account already exists.'}),409
@app.post('/api/auth/login')
def login():
    data=request.get_json(force=True); email=(data.get('email') or '').strip().lower(); password=data.get('password') or ''
    con=db(); row=con.execute('SELECT * FROM users WHERE email=?',(email,)).fetchone(); con.close()
    if not row or not check_password_hash(row['password_hash'],password):return jsonify({'ok':False,'error':'Invalid email or password.'}),401
    session['uid']=row['id']; return jsonify({'ok':True,'user':dict_user(row)})
def dict_user(row): return {'email':row['email'],'zone':row['zone'],'language':row['language'],'whatsapp':row['whatsapp'],'alerts':bool(row['alerts']),'email_alerts':bool(row['email_alerts']),'whatsapp_alerts':bool(row['whatsapp_alerts'])}
@app.post('/api/auth/logout')
def logout(): session.clear(); return jsonify({'ok':True})
@app.get('/api/auth/me')
def me():
    uid=session.get('uid');
    if not uid:return jsonify({'logged_in':False})
    con=db(); row=con.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone(); con.close()
    return jsonify({'logged_in':bool(row),'user':dict_user(row) if row else None})
@app.post('/api/profile')
def profile():
    uid=session.get('uid')
    if not uid:
        return jsonify({'ok':False,'error':'Login required.'}),401
    data=request.get_json(force=True)
    zone=data.get('zone') if data.get('zone') in STATIONS else 'sylhet'
    lang=data.get('language') if data.get('language') in {'en','bn'} else 'en'
    wa=(data.get('whatsapp') or '').strip()
    alerts=1 if data.get('alerts') else 0
    email_alerts=1 if data.get('email_alerts',True) else 0
    whatsapp_alerts=1 if data.get('whatsapp_alerts',True) else 0

    con=db()
    old=con.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()
    if not old:
        con.close()
        return jsonify({'ok':False,'error':'Account not found.'}),404
    old_zone=old['zone'] if old['zone'] in STATIONS else 'sylhet'
    old_alerts=bool(old['alerts'])
    con.execute('UPDATE users SET zone=?,language=?,whatsapp=?,alerts=?,email_alerts=?,whatsapp_alerts=? WHERE id=?',(zone,lang,wa,alerts,email_alerts,whatsapp_alerts,uid))
    row=con.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()
    con.commit(); con.close()

    # Immediate notification events: never wait for the background polling loop.
    immediate=[]
    z=package_station(zone)
    if alerts and not old_alerts:
        # Enabling alerts is a new subscription: send welcome immediately.
        _set_alert_state(uid,last_risk=z['risk'],welcome_sent=0,last_daily_date=None)
        immediate.append(_dispatch_user_event(row,'welcome',f"enable-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}",'welcome',z))
        if immediate[-1].get('sent'):
            _set_alert_state(uid,last_risk=z['risk'],welcome_sent=1)
    elif alerts and old_alerts and old_zone != zone:
        # Zone change while already subscribed: send one zone-update mail immediately.
        immediate.append(_dispatch_user_event(row,'zone_change',f'{old_zone}->{zone}','change',z))
        _set_alert_state(uid,last_risk=z['risk'])

    return jsonify({'ok':True,'user':dict_user(row),'email_configured':_email_configured(),'whatsapp_configured':_whatsapp_bridge_configured(),'immediate':immediate})

@app.post('/api/alerts/test-email')
def test_email():
    uid=session.get('uid')
    if not uid:return jsonify({'ok':False,'error':'Login required.'}),401
    con=db(); row=con.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone(); con.close()
    if not row:return jsonify({'ok':False,'error':'Account not found.'}),404
    z=package_station(row['zone'] if row['zone'] in STATIONS else 'sylhet')
    body=_message_for(z,row['language'] or 'en','daily')
    ok,detail=_send_email(row['email'],'FloodGuard BD — Test email',body)
    return jsonify({'ok':ok,'detail':detail})

@app.post('/api/alerts/enable')
def enable_alerts():
    uid=session.get('uid')
    if not uid:return jsonify({'ok':False,'error':'Login required.'}),401
    con=db(); row=con.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone(); con.close()
    if not row:return jsonify({'ok':False,'error':'Account not found.'}),404
    z=package_station(row['zone'] if row['zone'] in STATIONS else 'sylhet')
    _set_alert_state(uid,last_risk=z['risk'],welcome_sent=0,last_daily_date=None)
    fresh=con=None
    con=db(); row=con.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone(); con.close()
    result=_dispatch_user_event(row,'welcome',f"enable-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}",'welcome',z)
    if result.get('sent'):
        _set_alert_state(uid,last_risk=z['risk'],welcome_sent=1)
    return jsonify({'ok':bool(result.get('sent')),'message':'Welcome alert sent immediately.' if result.get('sent') else 'Could not send welcome alert.','details':result})

@app.get('/api/alerts/feed')
def alerts_feed():
    uid=session.get('uid')
    if not uid:return jsonify({'ok':False,'error':'Login required.'}),401
    con=db(); rows=con.execute('SELECT event_type,event_key,sent_at FROM alert_events WHERE user_id=? ORDER BY id DESC LIMIT 30',(uid,)).fetchall(); state=con.execute('SELECT * FROM alert_state WHERE user_id=?',(uid,)).fetchone(); con.close()
    return jsonify({'ok':True,'events':[dict(r) for r in rows],'state':dict(state) if state else None})

@app.post('/api/alerts/dispatch')
def alerts_dispatch():
    # Manual/cron-safe trigger for notification processing.
    return jsonify({'ok':True,'results':dispatch_alerts()})


if os.environ.get('ENABLE_BACKGROUND_ALERTS','false').lower() == 'true':
    start_alert_loop()
# GloFAS refresh is demand-driven by /api/live-refresh; this avoids consuming the Free instance during boot.

if __name__=='__main__':
    start_alert_loop()
    port=int(os.environ.get('PORT','5081')); app.run(host='0.0.0.0',port=port,debug=False)
