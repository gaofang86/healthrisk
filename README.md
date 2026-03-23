# Probabilistic Climate Risk Forecasting & Decision Optimization Platform

> Early warning system for climate-sensitive disease outbreaks (dengue) using a Medallion pipeline on Databricks, a mechanistic-ML hybrid suitability model, and Monte Carlo resource allocation.

---

## Overview

Dengue and other vector-borne diseases are highly sensitive to climate conditions, yet public health systems often lack reliable, data-driven early warning tools. This project builds an end-to-end production pipeline that ingests 2.34M+ daily climate records, engineers a Vector Suitability Index (VSI) grounded in epidemiological mechanisms, trains a LightGBM classifier with F2-optimized threshold and isotonic calibration, and frames district-level resource allocation as a 0/1 knapsack problem solved under Monte Carlo uncertainty.

---

## Architecture

```
NASA POWER API (daily)
        │
        ▼
  Bronze Layer          bronze.nasa_weather        2.34M+ daily records
  (raw ingestion)       bronze.dengue_raw           OpenDengue V1.3
        │
        ▼
  Silver Layer          silver.climate_features     Weekly aggregates
  (feature eng.)        silver.dengue_features      Province → grid mapping
        │
        ▼
  Gold Layer            gold.climate_features_vp    VSI + lag features
  (model-ready)         gold.training_dataset       Climate × dengue joined
        │
        ▼
  04_train_model        LightGBM + NBR baseline
        │
        ▼
  05_monte_carlo        0/1 knapsack under uncertainty
        │
        ▼
  FastAPI + Streamlit   Dashboard + FAISS RAG
```

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
│   │   └── preprocess.py            # Dengue cleaning, province→grid mapping
│   ├── training/
│   │   └── training.py              # Training dataset construction
│   ├── Decision/
│   │   └── monte_carlo.py           # Knapsack + MC uncertainty
│   └── utils/
└── config.py
```

---

## Key Components

### 1. Medallion Pipeline (Databricks + Delta Lake)

- **Bronze**: Raw NASA POWER daily records (T2M, PRECTOTCORR, RH2M) ingested via async tile-based API calls with retry/backoff. 2.34M+ rows across 754 grid points, 2017–2026.
- **Silver**: Daily → weekly aggregation. Temperature stats (mean, std, max), precipitation (sum, max, p90), humidity (mean, std), seasonal encoding (sin/cos).
- **Gold**: Mechanistic Vector Suitability Index (VSI) computed per grid-week, lag features (1–2 week), interaction terms, joined with province-level dengue case labels.

### 2. Vector Suitability Index (VSI)

A mechanistic-ML hybrid score combining three data-driven components:

```
temp_score     = -|z-score(temp_mean_C)|       # penalises deviation from optimal range
humidity_score = -|z-score(humidity_mean)|      # penalises deviation from optimal range
precip_z       = z-score(log1p(precip_sum_7d))  # standardised precipitation

VSI = temp_score + humidity_score + precip_z
```

The additive formulation (vs. multiplicative) ensures no single factor dominates and allows independent contribution from each climate driver — consistent with dengue transmission biology.

### 3. LightGBM Classifier

- **Target**: `outbreak_label` — binary flag when monthly case count exceeds 1.5× rolling 4-month average
- **Split**: time-based (train < 2022, test ≥ 2022) to prevent data leakage
- **Threshold**: F2-optimised (prioritises recall — missing an outbreak is costlier than a false alarm)
- **Calibration**: Isotonic regression for reliable probability outputs
- **Baseline**: Negative Binomial Regression (appropriate for overdispersed count data)

### 4. Monte Carlo Resource Allocation

District-level dengue resource allocation framed as a 0/1 knapsack problem. Model output probabilities are treated as uncertain inputs, sampled via Monte Carlo to produce allocation decisions that are robust under uncertainty.

### 5. FAISS-powered RAG

Historical outbreak analog retrieval using FAISS vector index. Given current climate conditions, the system retrieves the most similar past outbreak events to provide contextual decision support.

---

## Data Sources

| Source | Description | Coverage |
|--------|-------------|----------|
| [NASA POWER](https://power.larc.nasa.gov/) | Daily climate (T2M, precipitation, humidity) | 2017–2026, 1° grid |
| [OpenDengue V1.3](https://github.com/OpenDengue/master-repo) | Dengue case counts by province/month | 2017–2024, 8 SEA countries |

---

## Tech Stack

`Python` · `PySpark` · `Databricks` · `Delta Lake` · `LightGBM` · `FAISS` · `FastAPI` · `Streamlit` · `Docker` · `Hugging Face`

---

## Setup

```bash
# Clone the repo
git clone https://github.com/gaofang86/healthrisk.git

# In Databricks, add to sys.path
import sys
sys.path.insert(0, "/Workspace/healthrisk")

# Run notebooks in order
00_setup → 02_feature_engineering → 03_training_dataset → 04_train_model → 05_monte_carlo_policy
```

---

## Results

| Metric | LightGBM | NBR Baseline |
|--------|----------|--------------|
| F2 Score | — | — |
| Recall | — | — |
| Precision | — | — |
| AUC-ROC | — | — |

*Results to be updated after model training is complete.*

---

## Limitations & Future Work

- Dengue labels are at province/national level; finer spatial resolution would improve grid-level precision
- Climate data covers 2017–2026; dengue labels available through 2025 (OpenDengue V1.3 release lag)
- NDVI and land-use covariates not included in current feature set
- Expanding to South America (Brazil, Colombia) would substantially increase training data volume
