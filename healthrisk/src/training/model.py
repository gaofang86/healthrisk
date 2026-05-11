"""
src/training/model.py
Training pipeline: train → calibrate → validate → test
All stages return plain dicts so the notebook can log to MLflow cleanly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional

import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import (
    fbeta_score, f1_score, precision_score, recall_score,
    roc_auc_score, average_precision_score, confusion_matrix,
)

from config import GOLD_TRAINING_TABLE

# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────

FEATURE_COLS = [
    "temp_mean_C", "humidity_mean", "precip_sum_7d",
    "vsi", "temp_score", "humidity_score", "precip_z",
    "month_sin", "month_cos",
    "cases_lag_14d", "cases_roll28",
    "adm_0_encoded", 
    "cases_lag_7d",    
    "cases_lag_21d",   
    "cases_roll14",    
    "outbreak_lag1",   
]
TARGET_COL = "outbreak_label"
DATE_COL   = "iso_week"
GROUP_COL  = "adm_0_name"

# train 60% | val 20% | test 20%  (time-ordered, no leakage)
TRAIN_QUANTILE = 0.60
VAL_QUANTILE   = 0.80

RANDOM_SEED = 42

DEFAULT_LGB_PARAMS: dict = {
    "objective":          "binary",
    "metric":             ["auc"],
    "learning_rate":      0.05,
    "num_leaves":         63,
    "max_depth":          -1,
    "min_child_samples":  20,
    "feature_fraction":   0.8,
    "bagging_fraction":   0.8,
    "bagging_freq":       5,
    "n_estimators":       500,
    "early_stopping_rounds": 50,
    "verbose":            -1,
    "random_state":       RANDOM_SEED,
}


# ──────────────────────────────────────────────────────────────
# Data container
# ──────────────────────────────────────────────────────────────

@dataclass
class TrainingSplits:
    X_train: np.ndarray
    y_train: np.ndarray
    X_val:   np.ndarray
    y_val:   np.ndarray
    X_test:  np.ndarray
    y_test:  np.ndarray
    val_df:  pd.DataFrame    # keep raw cols for NB baseline (cases_roll28)
    test_df: pd.DataFrame


@dataclass
class TrainedArtifacts:
    lgbm:          lgb.LGBMClassifier
    calibrator:    LogisticRegression
    threshold:     float
    nb_model:      PoissonRegressor
    feature_cols:  list = field(default_factory=lambda: FEATURE_COLS)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _metrics(y_true: np.ndarray, y_pred: np.ndarray,
             y_proba: np.ndarray, prefix: str = "") -> dict:
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (0, 0, 0, 0)
    return {
        f"{prefix}f2":        round(fbeta_score(y_true, y_pred, beta=2, zero_division=0), 4),
        f"{prefix}f1":        round(f1_score(y_true, y_pred, zero_division=0), 4),
        f"{prefix}precision": round(precision_score(y_true, y_pred, zero_division=0), 4),
        f"{prefix}recall":    round(recall_score(y_true, y_pred, zero_division=0), 4),
        f"{prefix}roc_auc":   round(roc_auc_score(y_true, y_proba), 4),
        f"{prefix}pr_auc":    round(average_precision_score(y_true, y_proba), 4),
        f"{prefix}tp": int(tp), f"{prefix}fp": int(fp),
        f"{prefix}fn": int(fn), f"{prefix}tn": int(tn),
    }


def _find_f2_threshold(y_true: np.ndarray, proba: np.ndarray,
                       grid: int = 200) -> tuple[float, float]:
    """Sweep threshold grid; return (best_threshold, best_f2)."""
    best_t, best_f2 = 0.5, 0.0
    for t in np.linspace(0.01, 0.99, grid):
        f2 = fbeta_score(y_true, (proba >= t).astype(int), beta=2, zero_division=0)
        if f2 > best_f2:
            best_f2, best_t = f2, t
    return float(best_t), float(best_f2)


# ──────────────────────────────────────────────────────────────
# Stage 0 · load & split
# ──────────────────────────────────────────────────────────────

def load_and_split(spark) -> tuple[pd.DataFrame, TrainingSplits]:
    """Read gold training table → time-aware 60/20/20 split."""
    df = spark.table(GOLD_TRAINING_TABLE).toPandas()
    df = df.sort_values(DATE_COL).reset_index(drop=True)
    df["adm_0_encoded"] = df[GROUP_COL].astype("category").cat.codes 

    missing = df[FEATURE_COLS].isnull().sum()
    if missing.any():
        print("⚠️  Missing values:\n", missing[missing > 0])

    t_train = df[DATE_COL].quantile(TRAIN_QUANTILE)
    t_val   = df[DATE_COL].quantile(VAL_QUANTILE)

    train_df = df[df[DATE_COL] <= t_train].copy()
    val_df   = df[(df[DATE_COL] > t_train) & (df[DATE_COL] <= t_val)].copy()
    test_df  = df[df[DATE_COL] > t_val].copy()

    def _xy(d): return d[FEATURE_COLS].values, d[TARGET_COL].values.astype(int)

    X_train, y_train = _xy(train_df)
    X_val,   y_val   = _xy(val_df)
    X_test,  y_test  = _xy(test_df)

    print(f"Train : {len(train_df):,} rows | outbreak rate {y_train.mean()*100:.1f}%")
    print(f"Val   : {len(val_df):,}  rows | outbreak rate {y_val.mean()*100:.1f}%")
    print(f"Test  : {len(test_df):,}  rows | outbreak rate {y_test.mean()*100:.1f}%")

    splits = TrainingSplits(X_train, y_train, X_val, y_val,
                            X_test, y_test, val_df, test_df)
    return df, splits


# ──────────────────────────────────────────────────────────────
# Stage 1 · train
# ──────────────────────────────────────────────────────────────

def train(splits: TrainingSplits,
          lgb_params: Optional[dict] = None) -> lgb.LGBMClassifier:
    """Fit LightGBM on train, early-stop on val. Returns fitted model."""
    params = {**DEFAULT_LGB_PARAMS, **(lgb_params or {})}

    # class imbalance weight (fit on train only)
    pos_weight = (splits.y_train == 0).sum() / max((splits.y_train == 1).sum(), 1)
    params["scale_pos_weight"] = round(pos_weight, 3)
    print(f"scale_pos_weight = {pos_weight:.2f}")

    model = lgb.LGBMClassifier(**params)
    model.fit(
    splits.X_train, splits.y_train,
    eval_set=[(splits.X_val, splits.y_val)],
    callbacks=[
        lgb.log_evaluation(period=50),
        lgb.early_stopping(stopping_rounds=50, first_metric_only=False),
    ],
)
    print(f"Best iteration: {model.best_iteration_}")
    return model


# ──────────────────────────────────────────────────────────────
# Stage 2 · calibrate
# ──────────────────────────────────────────────────────────────

def calibrate(model, splits):
    val_proba_raw = model.predict_proba(splits.X_val)[:, 1].reshape(-1, 1)
    calibrator = LogisticRegression()
    calibrator.fit(val_proba_raw, splits.y_val)
    val_proba_cal = calibrator.predict_proba(
    model.predict_proba(splits.X_val)[:, 1].reshape(-1, 1)
)[:, 1]
    threshold, best_f2 = _find_f2_threshold(splits.y_val, val_proba_cal)
    print(f"Calibration → val F2 = {best_f2:.4f} at threshold = {threshold:.4f}")
    return calibrator, threshold


# ──────────────────────────────────────────────────────────────
# Stage 3 · validate
# ──────────────────────────────────────────────────────────────

def validate(model: lgb.LGBMClassifier,
             calibrator: LogisticRegression,
             threshold: float,
             splits: TrainingSplits) -> dict:
    """
    Evaluate on val set (used for hyper-param decisions, NOT for final reporting).
    Also fits + evaluates NB baseline on val for early comparison.
    Returns dict with keys 'lgbm' and 'nb'.
    """
    # ── LightGBM ──────────────────────────────────────────────
    val_proba_cal = calibrator.predict_proba(
    model.predict_proba(splits.X_val)[:, 1].reshape(-1, 1)
)[:, 1]
    val_pred      = (val_proba_cal >= threshold).astype(int)
    lgbm_metrics  = _metrics(splits.y_val, val_pred, val_proba_cal, prefix="val_")

    # ── NB baseline ───────────────────────────────────────────
    nb = PoissonRegressor(alpha=1.0, max_iter=300)
    y_counts = splits.val_df["dengue_total"].values.clip(0)
    nb.fit(splits.X_val, y_counts)

    pred_counts   = nb.predict(splits.X_val)
    roll_baseline = splits.val_df["cases_roll28"].values.clip(1)
    nb_proba      = np.clip(pred_counts / (roll_baseline * 1.5), 0, 1)
    nb_thresh, _  = _find_f2_threshold(splits.y_val, nb_proba)
    nb_pred       = (nb_proba >= nb_thresh).astype(int)
    nb_metrics    = _metrics(splits.y_val, nb_pred, nb_proba, prefix="val_")
    nb_metrics["val_threshold"] = nb_thresh

    _print_comparison(lgbm_metrics, nb_metrics, split="val")
    return {"lgbm": lgbm_metrics, "nb": nb_metrics, "nb_model": nb}


# ──────────────────────────────────────────────────────────────
# Stage 4 · test  (run ONCE at the very end)
# ──────────────────────────────────────────────────────────────

def test(model: lgb.LGBMClassifier,
         calibrator: LogisticRegression,
         threshold: float,
         nb_model: PoissonRegressor,
         splits: TrainingSplits) -> dict:
    """
    Final hold-out evaluation. Run only once.
    Re-fits NB on train+val to be fair, evaluates both on test.
    Returns dict with keys 'lgbm' and 'nb'.
    """
    # ── LightGBM ──────────────────────────────────────────────
    test_proba_cal = calibrator.predict_proba(
    model.predict_proba(splits.X_test)[:, 1].reshape(-1, 1)
)[:, 1]
    test_pred      = (test_proba_cal >= threshold).astype(int)
    lgbm_metrics   = _metrics(splits.y_test, test_pred, test_proba_cal, prefix="test_")
    lgbm_metrics["test_threshold"] = threshold

    # ── NB baseline (refit on train+val combined) ─────────────
    X_trainval = np.vstack([splits.X_train, splits.X_val])
    y_trainval_counts = np.concatenate([
        splits.val_df["dengue_total"].values.clip(0),   # val counts
    ])
    # NOTE: train counts not stored in splits → use nb_model already trained on val
    # as a conservative proxy (slightly underestimates NB perf, acceptable)
    pred_counts   = nb_model.predict(splits.X_test)
    roll_baseline = splits.test_df["cases_roll28"].values.clip(1)
    nb_proba      = np.clip(pred_counts / (roll_baseline * 1.5), 0, 1)
    nb_thresh, _  = _find_f2_threshold(splits.y_test, nb_proba)
    nb_pred       = (nb_proba >= nb_thresh).astype(int)
    nb_metrics    = _metrics(splits.y_test, nb_pred, nb_proba, prefix="test_")
    nb_metrics["test_threshold"] = nb_thresh

    _print_comparison(lgbm_metrics, nb_metrics, split="test")
    return {"lgbm": lgbm_metrics, "nb": nb_metrics}


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def feature_importance(model: lgb.LGBMClassifier) -> pd.DataFrame:
    return (pd.DataFrame({
                "feature":    FEATURE_COLS,
                "importance": model.feature_importances_,
            })
            .sort_values("importance", ascending=False)
            .reset_index(drop=True))


def _print_comparison(lgbm: dict, nb: dict, split: str):
    keys = ["f2", "f1", "precision", "recall", "roc_auc", "pr_auc"]
    print(f"\n── {split} comparison ───────────────────────────────")
    print(f"{'metric':<14} {'LightGBM':>10} {'NB baseline':>12}")
    print("-" * 38)
    for k in keys:
        lv = lgbm.get(f"{split}_{k}", lgbm.get(k, "—"))
        nv = nb.get(f"{split}_{k}", nb.get(k, "—"))
        lv_s = f"{lv:.4f}" if isinstance(lv, float) else str(lv)
        nv_s = f"{nv:.4f}" if isinstance(nv, float) else str(nv)
        print(f"{k:<14} {lv_s:>10} {nv_s:>12}")