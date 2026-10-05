import json
from model import MODEL_PATH,model_status,FEATURES

def explain_model():
    s=model_status(); out={"model":s,"features":FEATURES}
    if not s.get("ready"): return out
    try:
        import joblib
        m=joblib.load(MODEL_PATH)
        imp=getattr(m,"feature_importances_",None)
        if imp is not None: out["feature_importance"]={f:round(float(v),6) for f,v in zip(FEATURES,imp)}
    except Exception as e: out["error"]=str(e)
    return out
