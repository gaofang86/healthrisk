# HealthRisk: Dengue Early Warning & Community Mutual Aid

> Climate-sensitive disease risk · Monte Carlo uncertainty quantification · Community resource matching · Actionable public-private guidance

An end-to-end system that detects dengue outbreak risk from climate data, translates model probabilities into budget-constrained action plans, and connects vulnerable community members with local resources — with a React interface for both residents and government officers.

---

## The Problem

Most disease early warning systems stop at the alert. They tell you *that* something is coming. They don't tell you what to do about it, who should do it, with what budget, or where to go when it actually arrives.

More importantly, they treat the response as a government problem. In practice, communities have resources — cars, food, medical knowledge, spare time — that official programs can't mobilize fast enough. The gap between what government can deploy and what a sick family actually needs is often filled by neighbors, not agencies.

This system addresses both problems across four layers:

1. **Detection** — calibrated outbreak probability from climate and epidemiological data
2. **Allocation** — budget-constrained action plans for government and individuals
3. **Matching** — community resource board connecting those who have with those who need
4. **Guidance** — risk-adaptive interface that changes what it shows based on outbreak severity

---

## Architecture

```
NASA POWER API (daily)        OpenDengue V1.3
        │                           │
        ▼                           ▼
  Bronze Layer         bronze.nasa_weather (2.34M+ records)
  (raw ingestion)      bronze.dengue_raw
        │
        ▼
  Silver Layer         silver.climate_features (weekly aggregates)
  (feature eng.)       silver.dengue_features  (province-level)
        │
        ▼
  Gold Layer           gold.climate_features_vp (VSI + lag features)
  (model-ready)        gold.training_dataset    (climate × dengue)
        │
        ▼
  LightGBM + Platt Calibration
        │
        ▼
  Monte Carlo (1,000 simulations)
        │
        ▼
  Decision Layer
  ├── Government resource allocation  (0/1 knapsack, risk-adaptive)
  ├── Personal action plan            (0/1 knapsack)
  └── Local resource retrieval        (FAISS RAG)
        │
        ▼
  FastAPI Backend (Groq llama-3.3-70b)
        │
        ▼
  React Frontend
  ├── Resident view  — risk dashboard, community plaza, needs board, resource matching
  └── Government view — district overview, dynamic budget allocation, coverage gaps
```

---

## Key Components

### 1. Medallion Pipeline (Databricks + Delta Lake)

Climate data is ingested via async tile-based API calls across 170 tiles (5°×5° each), covering Southeast Asia and South America at 1° grid resolution. Each tile fetches three daily parameters — temperature (T2M), precipitation (PRECTOTCORR), and humidity (RH2M) — for 2017–2024 across ~4,250 coordinate points, with checkpoint-based retry logic to survive partial failures.

**Bronze → Silver → Gold:**

- **Bronze**: Raw daily records, no transformations. 2.34M+ rows.
- **Silver**: Daily → weekly aggregation. Temperature stats, precipitation, humidity, seasonal encoding (sin/cos). Province-level grid mapping via centroid matching.
- **Gold**: Mechanistic Vector Suitability Index (VSI), lag features (1–3 weeks), rolling case counts, country encoding. Joined with dengue case labels to produce the final training dataset (19,158 rows).

Key pipeline challenges:
- Time granularity mismatch between monthly dengue labels and weekly climate features required careful upsampling to avoid leakage
- Spatial collapse from grid coordinates to province boundaries via centroid mapping caused coverage loss that required iterative validation

### 2. Vector Suitability Index (VSI)

A mechanistic-ML hybrid score grounded in dengue transmission biology:

```python
temp_score     = -|z-score(temp_mean_C)|       # penalises deviation from optimal range
humidity_score = -|z-score(humidity_mean)|      # penalises deviation from optimal range
precip_z       = z-score(log1p(precip_sum_7d))  # standardised precipitation

VSI = temp_score + humidity_score + precip_z
```

The additive formulation ensures no single factor dominates, consistent with how climate conditions interact in dengue transmission.

### 3. LightGBM Classifier

- **Target**: Binary outbreak label — monthly case count exceeding 1.5× rolling 4-month average
- **Split**: Time-based (60/20/20) to prevent leakage
- **Calibration**: Platt scaling for reliable probability outputs (resolves isotonic regression plateau effect on test set)
- **Threshold**: 0.0888, F2-optimised — missing an outbreak is costlier than a false alarm
- **Baselines**: Negative Binomial Regression + Persistence model (outbreak_lag1)

One diagnostic finding shaped the final model significantly: temperature was the top feature for weeks, but it was proxying for geography — hot countries have more dengue, and the model had learned that coincidence as signal. Adding explicit country encoding (`adm_0_encoded`) dropped temperature to sixth place and pushed ROC AUC from 0.667 to 0.711. Switching early stopping from logloss to AUC monitoring extended training from 5 to 55 iterations. Adding four epidemiological lag features (`cases_lag_7d`, `cases_lag_21d`, `cases_roll14`, `outbreak_lag1`) produced the largest single improvement — ROC AUC +10%, PR AUC +80% — because outbreak serial continuity turned out to be the strongest predictive signal in the data.

**Final model hyperparameters:**
```python
DEFAULT_LGB_PARAMS = {
    "objective":              "binary",
    "metric":                 ["auc"],
    "learning_rate":          0.05,
    "num_leaves":             63,
    "min_child_samples":      20,
    "feature_fraction":       0.8,
    "bagging_fraction":       0.8,
    "bagging_freq":           5,
    "n_estimators":           500,
    "early_stopping_rounds":  50,
}
```

### 4. Decision Layer

The model outputs a calibrated probability. That number alone is not actionable. The decision layer translates it into something a government health officer or a person living in rural Indonesia can actually use.

**Step 1 — Monte Carlo uncertainty quantification**

The calibrated probability is sampled 1,000 times via Beta distribution to produce a risk distribution rather than a point estimate. Output is expressed as relative risk (e.g., 3.45× baseline) with a 95% confidence interval, which is more interpretable than a raw probability and more honest about uncertainty.

**Step 2 — Budget-constrained resource allocation (0/1 knapsack)**

Government and personal budgets are treated as separate knapsack constraints. Interventions are scored by expected risk reduction per dollar. The optimizer runs across all 1,000 Monte Carlo samples and maximizes expected risk reduction under uncertainty. Output includes specific action items, budget used, and projected risk reduction — dynamically adjusted for four risk tiers (low / moderate / high / critical).

**Step 3 — Local resource retrieval (FAISS RAG)**

A FAISS vector index retrieves the most relevant local clinics, hotlines, NGOs, and aid channels given the current district, risk tier, and disease type.

### 5. Community Mutual Aid System

The core insight behind the community layer: government programs fill gaps in bulk, but they can't mobilize fast enough for individuals. A neighbor with a car can get someone to a clinic in 20 minutes. A community food bank can deliver ORS the same day. The system makes this latent capacity visible and matchable.

**How it works:**

- Residents post resources they can offer (transport, food, medical advice, mosquito nets)
- Residents post needs they can't meet alone (clinic transport, food, BPJS guidance)
- Needs are prioritized by vulnerability: sick/recovering → elderly alone → unemployed/low income → no BPJS
- Matching surfaces the best available resource for each need
- Government dashboard shows coverage gaps — needs the community cannot fill — and deploys budget to fill them

**The gap-filling logic:**

```
Community resources cover needs → government sees what's left uncovered
→ government budget fills only the gaps, not what community already handles
→ no duplication, maximum marginal impact per dollar
```

This is the knapsack constraint applied at the community level: government intervention is only valuable where community supply is insufficient.

### 6. Risk-Adaptive Interface

The React frontend changes what it shows based on the current risk tier:

| Risk tier | Interface mode |
|-----------|---------------|
| Low | Routine prevention tips, community plaza, resource sharing |
| Moderate | Targeted action steps, elevated needs monitoring |
| High | Urgent guidance, needs board prioritized, government alert |
| Critical | Emergency protocols, hospital directions, full resource deployment |

Risk tier is derived from model predictions in `test_predictions.csv` — real outputs from the trained LightGBM model on historical test data (2022–2024).

---

## Results

**Test set performance (3 models compared):**

| Metric | LightGBM | NB Baseline | Persistence |
|--------|----------|-------------|-------------|
| F2 Score | **0.608** | 0.485 | 0.467 |
| F1 Score | **0.432** | 0.281 | 0.463 |
| Precision | **0.291** | 0.165 | 0.456 |
| Recall | **0.836** | 0.941 | 0.470 |
| ROC AUC | **0.814** | 0.515 | 0.680 |
| PR AUC | **0.474** | 0.169 | 0.301 |

Threshold: 0.0888 (F2-optimised). The persistence baseline (predict next week = this week) achieves ROC AUC 0.680, confirming that outbreaks have strong serial continuity. LightGBM exceeds this by 13 AUC points, meaning the climate and lag features together add meaningful signal beyond simple inertia.

**Top features by importance:**

| Feature | Importance | Notes |
|---------|------------|-------|
| cases_roll28 | 244 | Strongest signal — outbreaks are serially continuous |
| cases_lag_7d | 243 | 7-day lag case count |
| cases_lag_21d | 211 | 21-day lag case count |
| temp_mean_C | 208 | Climate signal (after geography confound removed) |
| month_cos | 201 | Seasonal periodicity |
| adm_0_encoded | 189 | Country-level baseline rates |

---

## Data Sources

| Source | Description | Coverage |
|--------|-------------|----------|
| [NASA POWER](https://power.larc.nasa.gov/) | Daily climate (T2M, precipitation, humidity) | 2017–2024, 1° grid |
| [OpenDengue V1.3](https://github.com/OpenDengue/master-repo) | Dengue case counts by province/month | 2017–2024, 7 countries |

Countries: Colombia, Peru, Bolivia, Indonesia, Panama, Ecuador, Argentina.

---

## Project Structure

```
healthrisk/
├── backend/                         # FastAPI backend
│   ├── main.py                      # /api/risk, /api/match, /api/ask endpoints
│   ├── requirements.txt
│   └── .env.example
├── frontend/                        # React + Vite frontend
│   ├── src/
│   │   └── App.jsx                  # Full single-page app
│   ├── public/
│   │   └── test_predictions.csv     # Real model predictions (test set)
│   ├── package.json
│   └── vercel.json
├── healthrisk/                      # ML pipeline (Databricks)
│   ├── app/
│   │   └── streamlit_app.py         # Original Streamlit prototype
│   ├── models/
│   │   ├── lgbm_model.pkl
│   │   └── platt_calibrator.pkl
│   ├── notebook/
│   │   ├── 00_setup.py
│   │   ├── 02_feature_engineering.py
│   │   ├── 03_training_dataset.py
│   │   ├── 04_train_model.py
│   │   └── 05_monte_carlo_policy.py
│   └── src/
│       ├── ingestion/
│       │   ├── climate_ingestion.py
│       │   └── dengue_ingestion.py
│       ├── features/
│       │   ├── feature_engineering.py
│       │   └── preprocess.py
│       ├── training/
│       │   ├── model.py
│       │   └── training.py
│       └── Decision/
│           ├── monte_carlo.py
│           ├── knapsack.py
│           └── resource_discovery.py
└── render.yaml                      # Render deployment config
```

---

## Local Development

### ML pipeline (Databricks)

```bash
git clone https://github.com/gaofang86/healthrisk.git

# In Databricks, add to sys.path
import sys
sys.path.insert(0, "/Workspace/healthrisk")

# Run notebooks in order
00_setup → 02_feature_engineering → 03_training_dataset → 04_train_model → 05_monte_carlo_policy
```

### Backend

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Add your GROQ_API_KEY (free at console.groq.com)

uvicorn main:app --reload
# Runs at http://localhost:8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# Runs at http://localhost:5173
```

---

## Deployment

**Backend** → [Render](https://render.com) (free tier, auto-deploys from `render.yaml`)

Set environment variable in Render dashboard: `GROQ_API_KEY`

**Frontend** → [Vercel](https://vercel.com) (free tier, auto-deploys from `frontend/`)

Update `frontend/vercel.json` with your Render URL before deploying.

---

## Limitations & Future Work

- Dengue labels are at province/national level; finer spatial resolution would improve grid-level precision
- Outbreak label is defined relative to local baseline (1.5× rolling average), not absolute case count — the model detects anomalies, not absolute severity
- Precision remains 0.291: roughly 1 in 3 alerts corresponds to a real outbreak; acceptable for public health screening but worth improving
- Test predictions cover 2022–2024; real-time inference would require connecting to NASA POWER live API (ingestion code already exists)
- Community matching is currently a prototype; real deployment would require user accounts and persistent storage
- NDVI and land-use covariates not yet included
- Expanding coverage beyond 7 countries would improve generalization
