from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from groq import Groq
import os
import joblib
import numpy as np
from pathlib import Path

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)
from dotenv import load_dotenv
load_dotenv()  
client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# ── load models ──────────────────────────────────────────────────
MODEL_DIR = Path(__file__).parent.parent / "healthrisk" / "models"
lgbm_model = joblib.load(MODEL_DIR / "lgbm_model.pkl")
calibrator  = joblib.load(MODEL_DIR / "platt_calibrator.pkl")

FEATURE_COLS = [
    "temp_mean_C", "humidity_mean", "precip_sum_7d",
    "vsi", "temp_score", "humidity_score", "precip_z",
    "month_sin", "month_cos",
    "cases_lag_14d", "cases_roll28",
    "adm_0_encoded",
    "cases_lag_7d", "cases_lag_21d", "cases_roll14", "outbreak_lag1",
]

COUNTRY_ENCODING = {
    "ARGENTINA": 0, "BOLIVIA": 1, "COLOMBIA": 2,
    "ECUADOR": 3,   "INDONESIA": 4, "PANAMA": 5, "PERU": 6,
}

def prob_to_tier(prob: float) -> tuple[str, int]:
    if prob < 0.15:   return "low",      0
    if prob < 0.35:   return "moderate", 1
    if prob < 0.55:   return "high",     2
    return                   "critical", 3

# ── 原有 prompts / models ────────────────────────────────────
SYSTEM_PROMPT = """You are a community health coordinator assistant for a dengue early warning system in Indonesia.
Your role is to help match people who need help with community members who can provide resources.
Be practical, warm, and concise. Always respond in the same language as the user's message.
For coordination advice, give 2-3 concrete steps. Keep responses under 150 words.
When relevant, mention urgency based on dengue risk level."""

class MatchRequest(BaseModel):
    need_name: str
    need_detail: str
    need_priority: str
    resource_provider: str
    resource_type: str
    risk_level: str
    language: str = "en"

class AskRequest(BaseModel):
    question: str
    context: str
    risk_level: str = "moderate"

class RiskRequest(BaseModel):
    country: str                  # e.g. "INDONESIA"
    month: int                    # 1-12
    temp_mean_C: float = 28.0
    humidity_mean: float = 75.0
    precip_sum_7d: float = 20.0
    vsi: float = 0.6
    temp_score: float = 0.7
    humidity_score: float = 0.6
    precip_z: float = 0.0
    cases_lag_7d: float = 5.0
    cases_lag_14d: float = 4.0
    cases_lag_21d: float = 3.0
    cases_roll28: float = 4.0
    cases_roll14: float = 4.5
    outbreak_lag1: float = 0.0

# ── routes ───────────────────────────────────────────────────

@app.get("/")
def health():
    return {"status": "ok", "service": "HealthRisk API"}

@app.post("/api/risk")
def predict_risk(req: RiskRequest):
    try:
        month_sin = np.sin(2 * np.pi * req.month / 12)
        month_cos = np.cos(2 * np.pi * req.month / 12)
        adm_0_encoded = COUNTRY_ENCODING.get(req.country.upper(), 4)

        features = np.array([[
            req.temp_mean_C, req.humidity_mean, req.precip_sum_7d,
            req.vsi, req.temp_score, req.humidity_score, req.precip_z,
            month_sin, month_cos,
            req.cases_lag_14d, req.cases_roll28,
            adm_0_encoded,
            req.cases_lag_7d, req.cases_lag_21d, req.cases_roll14, req.outbreak_lag1,
        ]])

        raw_prob  = lgbm_model.predict_proba(features)[:, 1][0]
        cal_prob  = calibrator.predict_proba([[raw_prob]])[0][1]
        tier, idx = prob_to_tier(cal_prob)

        return {
            "country":       req.country,
            "pred_prob":     round(float(cal_prob), 4),
            "risk_tier":     tier,
            "risk_tier_idx": idx,
            "relative_risk": round(float(cal_prob / 0.1912), 2),  # vs base rate
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/match")
def match_resources(req: MatchRequest):
    urgency = "URGENT — " if req.risk_level in ("high", "critical") else ""
    prompt = f"""{urgency}Community match needed:

Person needing help: {req.need_name}
Situation: {req.need_detail}
Priority: {req.need_priority}

Available resource: {req.resource_provider}
Can offer: {req.resource_type}

Current dengue risk level: {req.risk_level}

Provide:
1. A short coordination message the helper can send (2-3 sentences, friendly tone)
2. Two concrete next steps to complete this match
3. One safety note if risk is high or critical

Respond in {"Bahasa Indonesia" if req.language == "id" else "English"}."""

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            max_tokens=250,
            temperature=0.6,
        )
        return {"result": completion.choices[0].message.content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ask")
def ask_question(req: AskRequest):
    prompt = f"""Context: {req.context}. Risk level: {req.risk_level}.

User question: {req.question}

Answer helpfully and concisely (under 120 words). If risk is high or critical, emphasize urgency where relevant."""

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            max_tokens=200,
            temperature=0.6,
        )
        return {"result": completion.choices[0].message.content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))