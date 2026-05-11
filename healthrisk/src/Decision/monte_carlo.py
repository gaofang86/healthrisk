import numpy as np
import pandas as pd
from dataclasses import dataclass

@dataclass
class MCResult:
    base_prob:      float
    relative_risk:  float
    mean:           float
    p5:             float
    p95:            float
    risk_tier:      str
    samples:        np.ndarray

class MonteCarloEngine:

    def __init__(self, base_rate: float, n_sim: int = 1000):
        """
        base_rate: val set 正例比例 (0.1912)，作为基准风险
        n_sim:     模拟次数
        """
        self.base_rate = base_rate
        self.n_sim     = n_sim

    def run(self, base_prob: float) -> MCResult:
        relative_risk = base_prob / self.base_rate
    
        p          = np.clip(relative_risk / 4.0, 1e-4, 1 - 1e-4)
        confidence = 10
        alpha      = p * confidence
        beta_param = (1 - p) * confidence
    
        # 采样后还原回相对风险尺度
        samples_p  = np.random.beta(alpha, beta_param, self.n_sim)
        samples    = samples_p * 4.0  # 还原到相对风险尺度

        return MCResult(
        base_prob     = base_prob,
        relative_risk = relative_risk,
        mean          = float(samples.mean()),
        p5            = float(np.percentile(samples, 5)),
        p95           = float(np.percentile(samples, 95)),
        risk_tier     = self._classify(relative_risk),
        samples       = samples,
    )

    def run_batch(self, probs: np.ndarray) -> list[MCResult]:
        """对整个 test set 批量运行"""
        return [self.run(float(p)) for p in probs]

    def sensitivity_analysis(self, base_prob: float) -> dict:
        """
        搭便车效应展示：
        计算三种场景下的风险，用于 Streamlit 展示
        """
        gov_efficacy      = 0.60   # 大型消杀
        personal_efficacy = 0.45   # 个人防护组合

        r_gov      = self.run(base_prob * (1 - gov_efficacy))
        r_personal = self.run(base_prob * (1 - personal_efficacy))
        r_combined = self.run(
            base_prob * (1 - gov_efficacy) * (1 - personal_efficacy)
        )

        return {
            "gov_only":      r_gov.relative_risk,
            "personal_only": r_personal.relative_risk,
            "combined":      r_combined.relative_risk,
            "base":          base_prob / self.base_rate,
        }

    @staticmethod
    def _classify(relative_risk: float) -> str:
        if relative_risk > 3.0:   return "critical"
        elif relative_risk > 2.0: return "high"
        elif relative_risk > 1.0: return "moderate"
        else:                     return "low"