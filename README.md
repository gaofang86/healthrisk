# HealthRisk: Dengue Early Warning & Action Guidance

> Climate-sensitive disease risk · Monte Carlo uncertainty quantification · Actionable public-private guidance

An end-to-end agentic system that detects dengue outbreak risk from climate data and translates model probabilities into specific, budget-constrained action plans for both government agencies and individuals — with real local resource retrieval.

---

## The Problem

Most disease early warning systems stop at the alert. They tell you *that* something is coming. They don't tell you what to do about it, who should do it, with what budget, or where to go when it actually arrives.

This system addresses all four layers. It ingests 2.34M+ daily NASA climate records, trains a calibrated outbreak classifier, and feeds predictions into a decision layer that outputs actionable guidance — optimized under uncertainty, scoped to real budget constraints, and grounded in locally retrieved resources.

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
  ├── Government resource allocation  (0/1 knapsack)
  ├── Personal action plan            (0/1 knapsack)
  └── Local resource retrieval        (FAISS RAG)
        │
        ▼
  FastAPI + Streamlit Dashboard
```

---

## Key Components

### 1. Medallion Pipeline (Databricks + Delta Lake)

Climate data is ingested via async tile-based API calls across 170 tiles (5°×5° each), covering Southeast Asia and South America at 1° grid resolution. Each tile fetches three daily parameters — temperature (T2M), precipitation (PRECTOTCORR), and humidity (RH2M) — for 2017–2026 across ~4,250 coordinate points, with checkpoint-based retry logic to survive partial failures.

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
- **Split**: Time-based (train < 2022, val/test ≥ 2022) to prevent leakage
- **Calibration**: Platt scaling for reliable probability outputs (resolves isotonic regression plateau effect on test set)
- **Threshold**: F2-optimised — missing an outbreak is costlier than a false alarm
- **Baseline**: Negative Binomial Regression (appropriate for overdispersed count data)

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

The model outputs a calibrated probability — for example, 0.66 for a given district-week. That number alone is not actionable. The decision layer translates it into something a government health officer or a person living in rural Indonesia can actually use.

**Step 1 — Monte Carlo uncertainty quantification**

The calibrated probability is sampled 1,000 times via Beta distribution to produce a risk distribution rather than a point estimate. Output is expressed as relative risk (e.g., 3.45× baseline) with a 95% confidence interval, which is more interpretable than a raw probability and more honest about uncertainty.

**Step 2 — Budget-constrained resource allocation (0/1 knapsack)**

Government and personal budgets are treated as separate knapsack constraints. Interventions (e.g., community spraying, mosquito nets, repellent) are scored by expected risk reduction per dollar. The optimizer runs across all 1,000 Monte Carlo samples and maximizes expected risk reduction under uncertainty, not just at the point estimate. Output includes specific action items, budget used, and projected risk reduction.

**Step 3 — Local resource retrieval (FAISS RAG)**

The first two steps tell you what to do. This step tells you where to do it. A FAISS vector index retrieves the most relevant local clinics, hotlines, NGOs, and aid channels given the current district, risk tier, and disease type — filtered by country to avoid returning resources that are geographically irrelevant.

**The four questions the system answers:**

| Layer | Output |
|-------|--------|
| How dangerous is it? | Critical (3.45× baseline), 95% CI [2.68×, 3.95×] |
| What should government do? | Budget-optimised public intervention plan with projected impact |
| What can I do today? | Prioritised personal actions with cost and expected risk reduction |
| If an outbreak happens, where do I go? | Real local clinics, hotlines, NGO contacts |

### 5. FAISS-powered RAG

Historical outbreak analog retrieval using a FAISS vector index. Current climate conditions are used to retrieve the most similar past outbreak events for contextual decision support, alongside real-time local resource lookup.

---

## Results

**Test set performance (LightGBM vs. Negative Binomial baseline):**

| Metric | LightGBM | NB Baseline |
|--------|----------|-------------|
| F2 Score | 0.607 | 0.496 |
| F1 Score | 0.434 | 0.282 |
| Precision | 0.294 | 0.164 |
| Recall | 0.826 | 1.000 |
| ROC AUC | 0.813 | 0.527 |
| PR AUC | 0.457 | 0.173 |

Threshold: 0.109 (F2-optimised). Recall is intentionally prioritised — in a public health context, missing a real outbreak is more costly than a false alarm.

ROC AUC of 0.813 is within the range typically reported in academic dengue forecasting literature (0.75–0.85). The NB baseline recall of 1.000 reflects its strategy of flagging everything; the LightGBM model achieves 0.826 recall while substantially improving precision and discriminative ability.

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
| [NASA POWER](https://power.larc.nasa.gov/) | Daily climate (T2M, precipitation, humidity) | 2017–2026, 1° grid |
| [OpenDengue V1.3](https://github.com/OpenDengue/master-repo) | Dengue case counts by province/month | 2017–2024, 7 countries |

Countries: Colombia, Peru, Bolivia, Indonesia, Panama, Ecuador, Argentina.

---

## Project Structure

```
healthrisk/
├── notebook/
│   ├── 00_setup.py
│   ├── 02_feature_engineering.py
│   ├── 03_training_dataset.py
│   ├── 04_train_model.py
│   ├── 05_monte_carlo_policy.py
│   └── 99_explore_debug.py
├── src/
│   ├── ingestion/
│   │   ├── climate_ingestion.py     # NASA POWER async tile ingestion
│   │   └── dengue_ingestion.py      # OpenDengue V1.3 ingestion
│   ├── features/
│   │   ├── feature_engineering.py   # Silver + Gold climate pipeline
│   │   └── preprocess.py            # Dengue cleaning, province-grid mapping
│   ├── training/
│   │   └── training.py              # Training dataset construction
│   ├── Decision/
│   │   └── monte_carlo.py           # Knapsack + MC uncertainty
│   └── utils/
└── config.py
```

---

## Setup

```bash
git clone https://github.com/gaofang86/healthrisk.git

# In Databricks, add to sys.path
import sys
sys.path.insert(0, "/Workspace/healthrisk")

# Run notebooks in order
00_setup → 02_feature_engineering → 03_training_dataset → 04_train_model → 05_monte_carlo_policy
```

---

## Limitations & Future Work

- Dengue labels are at province/national level; finer spatial resolution would improve grid-level precision
- Outbreak label is defined relative to local baseline (1.5× rolling average), not absolute case count — the model detects anomalies, not absolute severity
- Precision remains low (0.294): roughly 1 in 3 alerts corresponds to a real outbreak; acceptable for a public health screening tool but worth improving
- NDVI and land-use covariates not yet included
- Expanding coverage beyond 7 countries would improve generalization
- Resource database for FAISS retrieval currently synthetic; integration with real health authority data would be needed for deployment
