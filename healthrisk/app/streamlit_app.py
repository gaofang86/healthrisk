print("=== APP STARTING ===", flush=True)
import sys
print("sys ok", flush=True)
from pathlib import Path
print("pathlib ok", flush=True)

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
print(f"ROOT={ROOT}", flush=True)

import streamlit as st
print("streamlit ok", flush=True)

import numpy as np
print("numpy ok", flush=True)

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
log.info(f"ROOT = {ROOT}")
log.info(f"sys.path = {sys.path[:3]}")

import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib

from Decision.monte_carlo import MonteCarloEngine
from Decision.knapsack import IncentivizedKnapsack
from Decision.resource_discovery import ResourceDiscovery

# ── 页面配置 ──────────────────────────────────────────
st.set_page_config(
    page_title="HealthRisk — Dengue Early Warning",
    page_icon="🦟",
    layout="wide"
)

# ── 加载模型（缓存）────────────────────────────────────
# 直接加载，不用 cache
model_dir  = ROOT / "models"
lgbm_model = joblib.load(model_dir / "lgbm_model.pkl")
calibrator = joblib.load(model_dir / "platt_calibrator.pkl")

engine    = MonteCarloEngine(base_rate=0.1912, n_sim=1000)
knapsack  = IncentivizedKnapsack()
discovery = ResourceDiscovery()

# ── Demo 数据（固定几个样本，不依赖 parquet）──────────
DEMO_SAMPLES = {
    "INDONESIA — High risk week (2021-12)": {
        "country": "INDONESIA", "pred_prob": 0.66, "iso_week": "2021-12-27"
    },
    "INDONESIA — Moderate risk week (2022-12)": {
        "country": "INDONESIA", "pred_prob": 0.28, "iso_week": "2022-12-26"
    },
    "PERU — Critical risk week (2023-03)": {
        "country": "PERU", "pred_prob": 0.71, "iso_week": "2023-03-27"
    },
    "PERU — Low risk week (2023-02)": {
        "country": "PERU", "pred_prob": 0.12, "iso_week": "2023-02-06"
    },
}

# ── Sidebar ───────────────────────────────────────────
st.sidebar.title("🦟 HealthRisk")
st.sidebar.markdown("Dengue Early Warning System")
st.sidebar.markdown("---")

selected = st.sidebar.selectbox(
    "Select district & week",
    options=list(DEMO_SAMPLES.keys()),
    index=0
)

sample      = DEMO_SAMPLES[selected]
country     = sample["country"]
sample_prob = sample["pred_prob"]
iso_week    = sample["iso_week"]

gov_budget = st.sidebar.slider(
    "Government budget (USD)",
    min_value=0, max_value=200000,
    value=50000, step=5000, format="$%d"
)

personal_budget = st.sidebar.slider(
    "Personal budget (USD)",
    min_value=0, max_value=500,
    value=200, step=10, format="$%d"
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Model performance**")
st.sidebar.caption("F2: 0.608 | ROC AUC: 0.814 | PR AUC: 0.474")
st.sidebar.caption("LightGBM + Platt calibration")
st.sidebar.caption("Trained on 2.34M NASA climate records")

# ── 计算 ──────────────────────────────────────────────

mc_result   = engine.run(sample_prob)
plan        = knapsack.solve(
    base_prob       = sample_prob,
    mc_samples      = mc_result.samples,
    gov_budget      = gov_budget,
    personal_budget = personal_budget,
)
resources   = discovery.get(country, mc_result.risk_tier)
sensitivity = engine.sensitivity_analysis(sample_prob)

# ── 主界面 ────────────────────────────────────────────
st.title("🦟 Dengue Early Warning & Action Guidance")
st.caption(
    "Climate-sensitive disease risk · "
    "Monte Carlo uncertainty quantification · "
    "Actionable public-private guidance"
)
st.markdown("---")

# Row 1：风险指标
tier_color = {
    "Low": "🟢", "Moderate": "🟡",
    "High": "🟠", "Critical": "🔴"
}
icon = tier_color.get(mc_result.risk_tier, "⚪")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("District", country)
with col2:
    st.metric("Risk tier", f"{icon} {mc_result.risk_tier}")
with col3:
    st.metric("Relative risk", f"{mc_result.relative_risk:.2f}x baseline")
with col4:
    st.metric("95% CI", f"{mc_result.p5:.2f}x – {mc_result.p95:.2f}x")

if mc_result.risk_tier in ("Critical", "High"):
    st.warning(
        "⚠️ Outbreak risk is elevated in this district. "
        "Seek care at first symptoms — do not wait."
    )

st.markdown("---")

# Row 2：行动方案 + 图表
left, right = st.columns([1, 1])

with left:
    st.subheader("🏛 Government action plan")
    if plan["public_plan"]["selected"]:
        for action, desc in zip(
            plan["public_plan"]["selected"],
            plan["public_plan"]["descriptions"]
        ):
            st.success(f"✓ **{action}**  \n{desc}")
        st.caption(
            f"Budget used: ${plan['public_plan']['total_cost']:,} / ${gov_budget:,} | "
            f"Risk reduction: {plan['public_plan']['risk_reduction']:.0%}"
        )
    else:
        st.info("No public actions within current budget.")

    st.markdown("---")

    st.subheader("🏠 What you can do today")
    if plan["personal_plan"]["selected"]:
        for i, (action, desc) in enumerate(zip(
            plan["personal_plan"]["selected"],
            plan["personal_plan"]["descriptions"]
        ), 1):
            st.info(f"**{i}. {action}**  \n{desc}")
        st.caption(
            f"Combined risk reduction: {plan['personal_plan']['risk_reduction']:.0%} | "
            f"Budget used: ${plan['personal_plan']['total_cost']:,} / ${personal_budget:,}"
        )
    else:
        st.info("Increase personal budget to unlock recommendations.")

with right:
    # 协同效应图
    st.subheader("📊 Synergistic effect of combined action")
    labels = ["No action", "Gov. only", "Personal only", "Combined"]
    values = [
        sensitivity["base"],
        sensitivity["gov_only"],
        sensitivity["personal_only"],
        sensitivity["combined"],
    ]
    colors = ["#E24B4A", "#F09595", "#85B7EB", "#1D9E75"]

    fig, ax = plt.subplots(figsize=(6, 3.5))
    bars = ax.bar(labels, values, color=colors, edgecolor="none", width=0.5)
    ax.axhline(y=1.0, linestyle="--", color="gray",
               linewidth=1, label="Baseline risk")
    ax.set_ylabel("Relative Risk (vs. baseline)")
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
    st.subheader("📈 Monte Carlo risk distribution")
    fig2, ax2 = plt.subplots(figsize=(6, 2.8))
    ax2.hist(mc_result.samples, bins=40,
             color="#85B7EB", edgecolor="none", alpha=0.8)
    ax2.axvline(mc_result.relative_risk, color="#E24B4A",
                linewidth=2,
                label=f"Estimate: {mc_result.relative_risk:.2f}x")
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

# Row 3：应对方案 + 本地资源
col_resp, col_res = st.columns([1, 1])

with col_resp:
    st.subheader("🚨 If outbreak occurs")
    for r in plan["response_actions"]:
        st.error(f"• **{r['name_en']}**  \n{r['description']}")

with col_res:
    st.subheader("📍 Local resources")
    res = resources["resources"]

    if res.get("clinics"):
        st.markdown("**🏥 Clinics**")
        for c in res["clinics"]:
            st.markdown(f"- {c}")

    if res.get("hotlines"):
        st.markdown("**📞 Hotlines**")
        for h in res["hotlines"]:
            st.markdown(f"- {h}")

    if res.get("ngos"):
        st.markdown("**🤝 NGOs**")
        for n in res["ngos"]:
            st.markdown(f"- {n}")

    if res.get("food"):
        st.markdown("**🍱 Food assistance**")
        for f in res["food"]:
            st.markdown(f"- {f}")

    if res.get("programs"):
        st.markdown("**📋 Programs**")
        for p in res["programs"]:
            st.markdown(f"- {p}")

    if res.get("vaccines"):
        st.markdown("**💉 Vaccines**")
        for v in res["vaccines"]:
            st.markdown(f"- {v}")
      
st.markdown("---")
st.subheader("💬 Ask a health question")

user_q = st.text_input(
    "Ask anything about dengue prevention or response",
    placeholder="e.g. What should I do if I have fever and rash?"
)

if user_q:
    context_summary = f"{country}, {mc_result.risk_tier} risk ({mc_result.relative_risk:.2f}x baseline)"
    q_lower = user_q.lower()
    
    if any(w in q_lower for w in ["fever", "sick", "symptoms", "rash"]):
        answer = f"In {country} with {mc_result.risk_tier} risk, fever + rash are key dengue symptoms. Go to the nearest clinic immediately. Call 119 ext 8 (Indonesia) or 113 (Peru)."
    elif any(w in q_lower for w in ["water", "mosquito", "breed", "container"]):
        answer = "Remove all standing water immediately. Even small containers like bottle caps can breed mosquitoes. Check your surroundings within 500m and clear within 24 hours."
    elif any(w in q_lower for w in ["dengue", "outbreak", "risk", "coming"]):
        answer = f"Based on climate data, {country} is currently at {mc_result.risk_tier} risk ({mc_result.relative_risk:.2f}x above baseline). The 95% confidence interval is [{mc_result.p5:.2f}x, {mc_result.p95:.2f}x]. Combined public and personal action can reduce risk to {mc_result.relative_risk * (1 - plan['personal_plan']['risk_reduction'] * 0.6):.2f}x."
    elif any(w in q_lower for w in ["hospital", "clinic", "doctor", "help"]):
        res = resources["resources"]
        clinics = res.get("clinics", [])
        answer = "Nearest resources: " + (clinics[0] if clinics else "Contact local health authority.")
    else:
        answer = f"Given {context_summary}, your top priorities are: {', '.join(plan['personal_plan']['selected'][:2]) if plan['personal_plan']['selected'] else 'consult local health authority'}. These reduce personal risk by {plan['personal_plan']['risk_reduction']:.0%}."
    
    st.info(answer)