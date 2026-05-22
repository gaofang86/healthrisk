# Databricks notebook source
import sys
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk/src")
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")

# COMMAND ----------

# MAGIC %pip install mlflow==2.13.0 typing_extensions==4.9.0 lightgbm --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.lightgbm
import mlflow.sklearn
import joblib, os

from training.model import (
    load_and_split,
    train,
    calibrate,
    validate,
    test,
    feature_importance,
    FEATURE_COLS,
    DEFAULT_LGB_PARAMS,
)
 
MLFLOW_EXPERIMENT = "/Users/gfine886@gmail.com/healthrisk_dengue"
MODEL_DIR         = "/dbfs/FileStore/healthrisk/models"
mlflow.set_experiment(MLFLOW_EXPERIMENT)

# COMMAND ----------

spark.sql("USE CATALOG healthrisk")
display(spark.table("gold.training_dataset"))
df, splits = load_and_split(spark)

# COMMAND ----------

print(spark.table("gold.training_dataset").columns)

# COMMAND ----------

with mlflow.start_run(run_name="LightGBM_train") as run:
    lgbm_model = train(splits)
 
    mlflow.log_params({
        **DEFAULT_LGB_PARAMS,
        "scale_pos_weight": lgbm_model.get_params()["scale_pos_weight"],
        "best_iteration":   lgbm_model.best_iteration_,
        "feature_cols":     ",".join(FEATURE_COLS),
    })
    mlflow.lightgbm.log_model(lgbm_model, "lgbm_model")
    train_run_id = run.info.run_id
 
print(f"Train run id: {train_run_id}")

# COMMAND ----------

with mlflow.start_run(run_name="LightGBM_calibrate") as run:
    calibrator, threshold = calibrate(lgbm_model, splits)
 
    mlflow.log_params({
        "calibration":    "platt",
        "threshold_from": "val_F2_grid_search",
    })
    mlflow.log_metric("val_threshold", threshold)
    mlflow.sklearn.log_model(calibrator, "platt_calibrator")
    calibrate_run_id = run.info.run_id
 
print(f"Calibrate run id: {calibrate_run_id}  |  threshold = {threshold:.4f}")
 

# COMMAND ----------

import importlib
import training.model
importlib.reload(training.model)
from training.model import validate, test, load_and_split, _print_comparison_three
with mlflow.start_run(run_name="LightGBM_validate") as run:
    val_results = validate(lgbm_model, calibrator, threshold, splits)
    mlflow.log_metrics({k: v for k, v in val_results["lgbm"].items()
                        if isinstance(v, (int, float)) and v == v})
    mlflow.log_metrics({f"nb_{k}": v for k, v in val_results["nb"].items()
                        if isinstance(v, (int, float)) and v == v})

nb_model     = val_results["nb_model"]
nb_threshold = val_results["nb"]["nb_threshold"]

# 在 with 块外面打印，不受 mlflow 输出干扰
_print_comparison_three(val_results["lgbm"], val_results["nb"],
                        val_results["persistence"], split="val")

# COMMAND ----------

# test comparison
with mlflow.start_run(run_name="LightGBM_test") as run:
    test_results = test(lgbm_model, calibrator, threshold,
                        nb_model, nb_threshold, splits)  # add nb_threshold
    
    mlflow.log_metrics(test_results["lgbm"])
    mlflow.log_metrics({f"nb_{k}": v for k, v in test_results["nb"].items()
                        if isinstance(v, (int, float))})
    # persistence record in mlflow
    mlflow.log_metrics({f"persistence_{k}": v 
                        for k, v in test_results["persistence"].items()
                        if isinstance(v, (int, float))})
    test_run_id = run.info.run_id

# COMMAND ----------

fi = feature_importance(lgbm_model)
print(fi.to_string(index=False))

# COMMAND ----------

# 看看不同 threshold 下 precision/recall 的变化
from sklearn.metrics import precision_recall_curve
import matplotlib.pyplot as plt
print(type(splits))
print(dir(splits))
X_test = splits.X_test
y_test = splits.y_test
y_prob_raw = lgbm_model.predict_proba(X_test)[:, 1]
y_prob_test = calibrator.predict(y_prob_raw.reshape(-1, 1))
prec, rec, thresholds = precision_recall_curve(y_test, y_prob_test)
plt.plot(rec, prec)
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("Precision-Recall Curve")
plt.show()
print("Train date range:", splits.X_train.shape)
print("Val date range:",   splits.X_val.shape)
print("Test date range:",  splits.X_test.shape)

# 看 evals result
print(lgbm_model.evals_result_)

# COMMAND ----------

import pandas as pd

# 获取预测概率和预测标签
y_prob_raw = lgbm_model.predict_proba(splits.X_test)[:, 1]
y_prob_cal = calibrator.predict(y_prob_raw.reshape(-1, 1))
y_pred = (y_prob_cal >= threshold).astype(int)

# 拼成一个可读的表
result_df = splits.test_df[["adm_0_name", "iso_week", "dengue_weekly", "outbreak_label"]].copy()
result_df["pred_prob"] = y_prob_cal
result_df["pred_label"] = y_pred
result_df["correct"] = (result_df["pred_label"] == result_df["outbreak_label"]).astype(int)

# 按概率排序
result_df = result_df.sort_values("pred_prob", ascending=False).reset_index(drop=True)

display(result_df.head(30))

# COMMAND ----------

sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk/src")
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")
spark.sql("USE CATALOG healthrisk")
val_proba_raw =  lgbm_model.predict_proba(splits.X_val)[:, 1]
print("val set 原始概率分布：")
print(pd.Series(val_proba_raw).describe())
print(f"\n唯一值数量：{pd.Series(val_proba_raw).nunique()}")

test_proba_raw =  lgbm_model.predict_proba(splits.X_test)[:, 1]
test_proba_cal = calibrator.predict_proba(test_proba_raw.reshape(-1, 1))[:, 1]
print("\ntest set 校准后概率分布：")
print(pd.Series(test_proba_cal).describe())
print(f"唯一值数量：{pd.Series(test_proba_cal).nunique()}")

# COMMAND ----------

test_proba_raw = lgbm_model.predict_proba(splits.X_test)[:, 1]
print("test set 原始概率（校准前）均值：", test_proba_raw.mean())
print("val set 原始概率均值：", val_proba_raw.mean())
print("test set outbreak_label 正例比例：", splits.y_test.mean())
print("val set outbreak_label 正例比例：", splits.y_val.mean())

# COMMAND ----------

print(f"A = {calibrator.coef_[0][0]:.6f}")
print(f"B = {calibrator.intercept_[0]:.6f}")

# COMMAND ----------

import os, joblib, pandas as pd

# 存到 repo 目录（这个你有写权限）
REPO_DIR = "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk"
MODEL_SAVE_DIR = f"{REPO_DIR}/models"
DATA_SAVE_DIR  = f"{REPO_DIR}/app/data"

os.makedirs(MODEL_SAVE_DIR, exist_ok=True)
os.makedirs(DATA_SAVE_DIR,  exist_ok=True)

joblib.dump(lgbm_model, f"{MODEL_SAVE_DIR}/lgbm_model.pkl")
joblib.dump(calibrator, f"{MODEL_SAVE_DIR}/platt_calibrator.pkl")

test_pred_df = splits.test_df.copy()
test_pred_df["pred_prob"]  = calibrator.predict_proba(
    lgbm_model.predict_proba(splits.X_test)[:, 1].reshape(-1, 1)
)[:, 1]
test_pred_df["pred_label"] = (test_pred_df["pred_prob"] >= threshold).astype(int)

test_pred_df.to_parquet(f"{DATA_SAVE_DIR}/test_predictions.parquet", index=False)

pd.DataFrame({
    "threshold":        [threshold],
    "base_rate":        [float(splits.y_val.mean())],
    "train_run_id":     [train_run_id],
    "calibrate_run_id": [calibrate_run_id],
}).to_csv(f"{MODEL_SAVE_DIR}/model_metadata.csv", index=False)

print("✅ Saved to repo")
print(os.listdir(MODEL_SAVE_DIR))
print(os.listdir(DATA_SAVE_DIR))

# COMMAND ----------

import os

for f in ["lgbm_model.pkl", "platt_calibrator.pkl"]:
    size = os.path.getsize(f"/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk/models/{f}")
    print(f"{f}: {size/1024/1024:.1f} MB")

size = os.path.getsize("/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk/app/data/test_predictions.parquet")
print(f"test_predictions.parquet: {size/1024/1024:.1f} MB")

# COMMAND ----------

# 保存 test predictions，避免 05 notebook 重新跑 load_and_split
import pandas as pd

test_pred_df = splits.test_df.copy()
test_pred_df["pred_prob"] = calibrator.predict_proba(
    lgbm_model.predict_proba(splits.X_test)[:, 1].reshape(-1, 1)
)[:, 1]
test_pred_df["outbreak_label"] = splits.y_test

spark.createDataFrame(test_pred_df) \
     .write.mode("overwrite") \
     .saveAsTable("gold.test_predictions")

pd.DataFrame({
    "threshold":    [threshold],
    "base_rate":    [float(splits.y_val.mean())],
    "train_run_id": [train_run_id],
    "test_run_id":  [test_run_id],
    "feature_cols": [",".join(FEATURE_COLS)],
}).to_csv(f"{MODEL_DIR}/model_metadata.csv", index=False)

print(f"Saved {len(test_pred_df)} test predictions to gold.test_predictions")
