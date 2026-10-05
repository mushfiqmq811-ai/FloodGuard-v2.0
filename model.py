"""Production model interface. No synthetic fallback is permitted."""
from pathlib import Path
from datetime import datetime, timezone
import json
import joblib
import numpy as np

MODEL_PATH = Path("model/real_flood_model.joblib")
META_PATH = Path("model/real_flood_model.json")

FEATURES = ["water_level", "water_level_6h_ago", "water_level_12h_ago", "water_level_24h_ago"]


def model_status():
    if not MODEL_PATH.exists():
        return {"ready": False, "reason": "REAL_MODEL_NOT_TRAINED", "path": str(MODEL_PATH)}
    try:
        meta=json.loads(META_PATH.read_text()) if META_PATH.exists() else {}
        return {"ready": True, "path": str(MODEL_PATH), **meta}
    except Exception as exc:
        return {"ready": False, "reason": f"MODEL_METADATA_ERROR: {exc}"}


def predict_flood(hyd, weather, danger):
    status=model_status()
    if not status["ready"]:
        return {
            "ready": False,
            "predicted_water_level_24h": None,
            "flood_probability": None,
            "risk": "UNAVAILABLE",
            "model": "No real-data-trained model available",
            "danger_level": danger,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "reason": status.get("reason"),
        }
    x=np.array([[float(hyd[k]) for k in FEATURES]],dtype=float)
    model=joblib.load(MODEL_PATH)
    predicted=float(model.predict(x)[0])
    # Classification is based on the official/reference danger threshold, not synthetic probabilities.
    if predicted >= danger + 1.0: risk="SEVERE"
    elif predicted >= danger: risk="FLOOD"
    elif predicted >= danger - 0.5: risk="WARNING"
    else: risk="NORMAL"
    return {
        "ready": True,
        "predicted_water_level_24h": round(predicted,2),
        "flood_probability": None,
        "risk": risk,
        "model": status.get("model_name","Real-data-trained model"),
        "danger_level": danger,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "training_source": status.get("training_source"),
    }
