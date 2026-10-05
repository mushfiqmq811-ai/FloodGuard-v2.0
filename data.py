"""Weather utility. Source failure is explicit; no synthetic fallback."""
import requests

def get_weather(lat, lon):
    url="https://api.open-meteo.com/v1/forecast"
    params={"latitude":lat,"longitude":lon,"current":"temperature_2m,rain,precipitation","hourly":"precipitation,rain,temperature_2m,soil_moisture_0_to_10cm","forecast_days":2,"timezone":"Asia/Dhaka"}
    r=requests.get(url,params=params,timeout=12,headers={"User-Agent":"FloodGuard-BD/5.0"}); r.raise_for_status(); j=r.json()
    hourly=j.get("hourly",{}); rain=hourly.get("rain",[]) or hourly.get("precipitation",[]); current=j.get("current",{})
    return {"source":"Open-Meteo live","temperature_c":current.get("temperature_2m"),"rain_now_mm":current.get("rain",current.get("precipitation",0)) or 0,"rain_next_24h_mm":round(sum(float(x or 0) for x in rain[:24]),1)}
