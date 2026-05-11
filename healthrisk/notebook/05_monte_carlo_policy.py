# Databricks notebook source
# MAGIC %pip install lightgbm

# COMMAND ----------

# MAGIC %pip install lightgbm mlflow scikit-learn

# COMMAND ----------

# MAGIC %pip install lightgbm mlflow scikit-learn faiss-cpu sentence-transformers

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk")
sys.path.insert(0, "/Workspace/Repos/gfine886@gmail.com/healthrisk/healthrisk/src")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from Decision.monte_carlo import MonteCarloEngine
from Decision.knapsack import IncentivizedKnapsack
from Decision.resource_discovery import ResourceDiscovery

# COMMAND ----------

import pandas as pd

spark.sql("USE CATALOG healthrisk")

base_rate = 0.1912
threshold = 0.0888

test_df        = spark.table("gold.test_predictions").toPandas()
test_proba_cal = test_df["pred_prob"].values

print(f"Base rate (val): {base_rate:.4f}")
print(f"Test samples loaded: {len(test_df)}")

# COMMAND ----------

engine  = MonteCarloEngine(base_rate=base_rate, n_sim=1000)
results = engine.run_batch(test_proba_cal)

tiers = pd.Series([r.risk_tier for r in results])
print(f"Samples processed: {len(results)}")
print("Risk tier distribution:")
print(tiers.value_counts())

# COMMAND ----------

high_risk_idx = np.argmax(test_proba_cal)
sample_prob   = float(test_proba_cal[high_risk_idx])
sensitivity   = engine.sensitivity_analysis(sample_prob)

labels = ["No intervention", "Gov. only", "Personal only", "Combined"]
values = [sensitivity["base"], sensitivity["gov_only"],
          sensitivity["personal_only"], sensitivity["combined"]]
colors = ["#E24B4A", "#F09595", "#85B7EB", "#1D9E75"]

fig, ax = plt.subplots(figsize=(8, 4))
bars    = ax.bar(labels, values, color=colors)
ax.set_ylabel("Relative Risk (vs. baseline)")
ax.set_title("Synergistic Effect of Public-Private Action")
ax.axhline(y=1.0, linestyle="--", color="gray", label="Baseline risk")
ax.legend()
for bar, val in zip(bars, values):
    ax.text(bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.05,
            f"{val:.2f}x", ha="center", fontsize=11)
plt.tight_layout()
plt.show()

# COMMAND ----------

from Decision.knapsack import IncentivizedKnapsack
from Decision.resource_discovery import ResourceDiscovery

idx        = np.argmax(test_proba_cal)
sample_prob = float(test_proba_cal[idx])
sample_mc   = engine.run(sample_prob)
country     = test_df.iloc[idx]["adm_0_name"]  # 注意：用 test_df 不是 splits.test_df

knapsack  = IncentivizedKnapsack()
plan      = knapsack.solve(
    base_prob       = sample_prob,
    mc_samples      = sample_mc.samples,
    gov_budget      = 50000,
    personal_budget = 200,
)

discovery = ResourceDiscovery()
resources = discovery.get(country, sample_mc.risk_tier)

# COMMAND ----------

import importlib
import Decision.monte_carlo as mc_module
importlib.reload(mc_module)
from Decision.monte_carlo import MonteCarloEngine

engine = MonteCarloEngine(base_rate=base_rate, n_sim=1000)

test_mc = engine.run(sample_prob)
print(f"relative_risk: {test_mc.relative_risk:.4f}")
print(f"samples mean:  {test_mc.samples.mean():.4f}")
print(f"samples max:   {test_mc.samples.max():.4f}")
print(f"p5:  {test_mc.p5:.4f}")
print(f"p95: {test_mc.p95:.4f}")

# COMMAND ----------

importlib.reload(mc_module)
from Decision.monte_carlo import MonteCarloEngine

engine = MonteCarloEngine(base_rate=base_rate, n_sim=1000)

test_mc = engine.run(sample_prob)
print(f"relative_risk: {test_mc.relative_risk:.4f}")
print(f"samples mean:  {test_mc.samples.mean():.4f}")
print(f"samples max:   {test_mc.samples.max():.4f}")
print(f"p5:  {test_mc.p5:.4f}")
print(f"p95: {test_mc.p95:.4f}")
print(f"Country: {country}")

print(f"District: {country}")
print(f"Risk tier: {sample_mc.risk_tier} ({sample_mc.relative_risk:.2f}x above baseline)")
print(f"95% CI: [{sample_mc.p5:.2f}x, {sample_mc.p95:.2f}x]")

if resources["urgency_note"]:
    print(f"\n{resources['urgency_note']}")

print("\n── What the government should do ──────────────")
for action, desc in zip(plan["public_plan"]["selected"],
                        plan["public_plan"]["descriptions"]):
    print(f"  ✓ {action:<35} → {desc}")
print(f"  Total budget used: ${plan['public_plan']['total_cost']:,}")

print("\n── What YOU can do today ───────────────────────")
for i, (action, desc) in enumerate(zip(plan["personal_plan"]["selected"],
                                        plan["personal_plan"]["descriptions"]), 1):
    print(f"  {i}. {action:<35} → {desc}")
print(f"  Combined risk reduction: {plan['personal_plan']['risk_reduction']:.0%}")

print("\n── If outbreak occurs ──────────────────────────")
for r in plan["response_actions"]:
    print(f"  • {r['name_en']:<35} → {r['description']}")

print("\n── Local resources ─────────────────────────────")
for c in resources["resources"]["clinics"]:
    print(f"  🏥 {c}")
for h in resources["resources"]["hotlines"]:
    print(f"  📞 {h}")
for n in resources["resources"]["ngos"]:
    print(f"  🤝 {n}")
for f in resources["resources"]["food"]:
    print(f"  🍱 {f}")
for p in resources["resources"]["programs"]:
    print(f"  📋 {p}")

# COMMAND ----------

# 生成校准后概率
test_proba_raw = lgbm_model.predict_proba(splits.X_test)[:, 1]
test_proba_cal = calibrator.predict_proba(
    test_proba_raw.reshape(-1, 1)
)[:, 1]

# 初始化 MC 引擎
engine = MonteCarloEngine(base_rate=base_rate, n_sim=1000)

# 批量运行
results = engine.run_batch(test_proba_cal)

print(f"Samples processed: {len(results)}")
print("Risk tier distribution:")
tiers = pd.Series([r.risk_tier for r in results])
print(tiers.value_counts())

# COMMAND ----------

from Decision.knapsack import IncentivizedKnapsack
from Decision.resource_discovery import ResourceDiscovery

# 取一个 high risk 样本
idx          = np.argmax(test_proba_cal)
sample_prob  = float(test_proba_cal[idx])
sample_mc    = engine.run(sample_prob)
country      = splits.test_df.iloc[idx]["adm_0_name"]

# 背包求解
knapsack     = IncentivizedKnapsack()
plan         = knapsack.solve(
    base_prob       = sample_prob,
    mc_samples      = sample_mc.samples,
    gov_budget      = 50000,
    personal_budget = 200,
)

# 资源查询
discovery    = ResourceDiscovery()
resources    = discovery.get(country, sample_mc.risk_tier)

# 打印 actionable guidance
print(f"District: {country}")
print(f"Risk tier: {sample_mc.risk_tier} ({sample_mc.relative_risk:.2f}x above baseline)")
print(f"95% CI: [{sample_mc.p5:.2f}x, {sample_mc.p95:.2f}x]")

if resources["urgency_note"]:
    print(f"\n{resources['urgency_note']}")

print("\n── What the government should do ──────────────")
for action, desc in zip(plan["public_plan"]["selected"],
                        plan["public_plan"]["descriptions"]):
    print(f"  ✓ {action:<30} → {desc}")
print(f"  Total budget used: ${plan['public_plan']['total_cost']:,}")

print("\n── What YOU can do today ───────────────────────")
for i, (action, desc) in enumerate(zip(plan["personal_plan"]["selected"],
                                        plan["personal_plan"]["descriptions"]), 1):
    print(f"  {i}. {action:<30} → {desc}")
print(f"  Combined personal risk reduction: "
      f"{plan['personal_plan']['risk_reduction']:.0%}")

print("\n── If outbreak occurs ──────────────────────────")
for r in plan["response_actions"]:
    print(f"  • {r['name_en']:<30} → {r['description']}")

print("\n── Local resources ─────────────────────────────")
for clinic in resources["resources"]["clinics"]:
    print(f"  🏥 {clinic}")
for hotline in resources["resources"]["hotlines"]:
    print(f"  📞 {hotline}")
for ngo in resources["resources"]["ngos"]:
    print(f"  🤝 {ngo}")
for food in resources["resources"]["food"]:
    print(f"  🍱 {food}")
