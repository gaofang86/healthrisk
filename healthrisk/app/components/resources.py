import sys
sys.path.insert(0, "../src")

import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib

from Decision.monte_carlo import MonteCarloEngine
from Decision.knapsack import IncentivizedKnapsack
from Decision.resource_discovery import ResourceDiscovery

# ── 页面配置 ──────────────────────────────────────
st.set_page_config(
    page_title="HealthRisk — Dengue Early Warning",
    page_icon="🦟",
    layout="wide"
)

# ── 加载模型和数据（缓存，只加载一次）─────────────
@st.cache_resource
def load_models():
    lgbm_model = joblib.load("/tmp/healthrisk/models/lgbm_model.pkl")
    calibrator  = joblib.load("/tmp/healthrisk/models/platt_calibrator.pkl")
    return lgbm_model, calibrator

@st.cache_data
def load_test_data():
    return pd.read_parquet("/tmp/healthrisk/data/test_predictions.parquet")

@st.cache_resource
def load_engines():
    engine    = MonteCarloEngine(base_rate=0.1912, n_sim=1000)
    knapsack  = IncentivizedKnapsack()
    discovery = ResourceDiscovery()
    return engine, knapsack, discovery

# ── Sidebar：用户输入 ─────────────────────────────
st.sidebar.title("🦟 HealthRisk Settings")
st.sidebar.markdown("---")

country = st.sidebar.selectbox(
    "Select district",
    options=["INDONESIA", "PERU"],
    index=0
)

gov_budget = st.sidebar.slider(
    "Government budget (USD)",
    min_value=0,
    max_value=200000,
    value=50000,
    step=5000,
    format="$%d"
)

personal_budget = st.sidebar.slider(
    "Personal budget (USD)",
    min_value=0,
    max_value=500,
    value=200,
    step=10,
    format="$%d"
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Model info**")
st.sidebar.caption("LightGBM + Platt calibration")
st.sidebar.caption("F2: 0.608 | ROC AUC: 0.814")

# ── 主界面 ────────────────────────────────────────
st.title("🦟 Dengue Early Warning & Action Guidance")
st.caption("Climate-sensitive disease risk · Monte Carlo uncertainty · Actionable guidance")
st.markdown("---")

# 加载资源
engine, knapsack, discovery = load_engines()

# 用选择的地区过滤 test data，取最高风险样本
try:
    test_df = load_test_data()
    district_df = test_df[test_df["adm_0_name"] == country]
    if district_df.empty:
        st.error(f"No data found for {country}")
        st.stop()
    
    # 取最高风险样本
    idx         = district_df["pred_prob"].idxmax()
    sample_prob = float(district_df.loc[idx, "pred_prob"])
    iso_week    = str(district_df.loc[idx, "iso_week"])[:10]

except Exception as e:
    # fallback：用固定值演示
    sample_prob = 0.66
    iso_week    = "2023-03-20"

# MC 计算
mc_result  = engine.run(sample_prob)
plan       = knapsack.solve(
    base_prob       = sample_prob,
    mc_samples      = mc_result.samples,
    gov_budget      = gov_budget,
    personal_budget = personal_budget,
)
resources  = discovery.get(country, mc_result.risk_tier)
sensitivity = engine.sensitivity_analysis(sample_prob)

# ── Row 1：风险概览 ───────────────────────────────
col1, col2, col3, col4 = st.columns(4)

tier_colors = {
    "Low":      "🟢",
    "Moderate": "🟡",
    "High":     "🟠",
    "Critical": "🔴"
}
icon = tier_colors.get(mc_result.risk_tier, "⚪")

with col1:
    st.metric("Risk tier", f"{icon} {mc_result.risk_tier}")
with col2:
    st.metric("Relative risk", f"{mc_result.relative_risk:.2f}x baseline")
with col3:
    st.metric("95% CI", f"{mc_result.p5:.2f}x – {mc_result.p95:.2f}x")
with col4:
    st.metric("Week", iso_week)

if mc_result.risk_tier in ("Critical", "High"):
    st.warning("⚠️ Outbreak risk is elevated. Seek care at first symptoms — do not wait.")

st.markdown("---")

# ── Row 2：左列行动方案 + 右列图表 ──────────────
left, right = st.columns([1, 1])

with left:
    # 政府行动
    st.subheader("🏛 Government action plan")
    if plan["public_plan"]["selected"]:
        for action, desc in zip(
            plan["public_plan"]["selected"],
            plan["public_plan"]["descriptions"]
        ):
            st.success(f"✓ **{action}**  \n{desc}")
        st.caption(f"Budget used: ${plan['public_plan']['total_cost']:,} / ${gov_budget:,}")
    else:
        st.info("No public actions within current budget.")

    st.markdown("---")

    # 个人行动
    st.subheader("🏠 What you can do today")
    if plan["personal_plan"]["selected"]:
        for i, (action, desc) in enumerate(zip(
            plan["personal_plan"]["selected"],
            plan["personal_plan"]["descriptions"]
        ), 1):
            st.info(f"**{i}. {action}**  \n{desc}")
        st.caption(
            f"Combined risk reduction: "
            f"{plan['personal_plan']['risk_reduction']:.0%} | "
            f"Budget used: ${plan['personal_plan']['total_cost']:,}"
        )
    else:
        st.info("Increase personal budget to unlock recommendations.")

with right:
    # 协同效应图
    st.subheader("📊 Synergistic effect")

    labels = ["No action", "Gov. only", "Personal only", "Combined"]
    values = [
        sensitivity["base"],
        sensitivity["gov_only"],
        sensitivity["personal_only"],
        sensitivity["combined"],
    ]
    colors = ["#E24B4A", "#F09595", "#85B7EB", "#1D9E75"]

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(labels, values, color=colors, edgecolor="none")
    ax.axhline(y=1.0, linestyle="--", color="gray", linewidth=1, label="Baseline")
    ax.set_ylabel("Relative Risk (vs. baseline)")
    ax.set_title("")
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.05,
            f"{val:.2f}x", ha="center", fontsize=10
        )
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

    # MC 分布图
    st.subheader("📈 Risk distribution (MC samples)")
    fig2, ax2 = plt.subplots(figsize=(6, 3))
    ax2.hist(mc_result.samples, bins=40, color="#85B7EB",
             edgecolor="none", alpha=0.8)
    ax2.axvline(mc_result.relative_risk, color="#E24B4A",
                linewidth=2, label=f"Point estimate: {mc_result.relative_risk:.2f}x")
    ax2.axvline(mc_result.p5,  color="gray", linewidth=1,
                linestyle="--", label=f"p5: {mc_result.p5:.2f}x")
    ax2.axvline(mc_result.p95, color="gray", linewidth=1,
                linestyle=":",  label=f"p95: {mc_result.p95:.2f}x")
    ax2.set_xlabel("Relative Risk")
    ax2.set_ylabel("Frequency")
    ax2.legend(fontsize=8)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    plt.tight_layout()
    st.pyplot(fig2)
    plt.close()

st.markdown("---")

# ── Row 3：应对方案 + 本地资源 ────────────────────
col_resp, col_res = st.columns([1, 1])

with col_resp:
    st.subheader("🚨 If outbreak occurs")
    for r in plan["response_actions"]:
        st.error(f"• **{r['name_en']}**  \n{r['description']}")

with col_res:
    st.subheader("📍 Local resources")

    res = resources["resources"]

    if res["clinics"]:
        st.markdown("**🏥 Clinics**")
        for c in res["clinics"]:
            st.markdown(f"- {c}")

    if res["hotlines"]:
        st.markdown("**📞 Hotlines**")
        for h in res["hotlines"]:
            st.markdown(f"- {h}")

    if res["ngos"]:
        st.markdown("**🤝 NGOs**")
        for n in res["ngos"]:
            st.markdown(f"- {n}")

    if res["food"]:
        st.markdown("**🍱 Food assistance**")
        for f in res["food"]:
            st.markdown(f"- {f}")

    if res["programs"]:
        st.markdown("**📋 Programs**")
        for p in res["programs"]:
            st.markdown(f"- {p}")