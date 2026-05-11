import pandas as pd
import numpy as np
from pathlib import Path
from itertools import combinations

DATA_PATH = Path(__file__).parent.parent.parent / "data" / "actions_seed.csv"

class IncentivizedKnapsack:

    def __init__(self, actions_path=DATA_PATH):
        self.actions = pd.read_csv(actions_path)

    def get_actions(self, phase=None, action_type=None):
        df = self.actions.copy()
        if phase:
            df = df[df["phase"] == phase]
        if action_type:
            df = df[df["type"] == action_type]
        return df

    def solve(self, base_prob, mc_samples, gov_budget, personal_budget):
        
        personal_prev = self.get_actions(phase="prevention", action_type="personal")
        public_prev   = self.get_actions(phase="prevention", action_type="public")
        response_all  = self.get_actions(phase="response")

        public_plan   = self._solve_single(public_prev,   gov_budget,      mc_samples)
        personal_plan = self._solve_single(personal_prev, personal_budget,  mc_samples)

        combined_reduction = 1 - (
            (1 - public_plan["risk_reduction"]) *
            (1 - personal_plan["risk_reduction"])
        )

        return {
            "public_plan":           public_plan,
            "personal_plan":         personal_plan,
            "response_actions":      response_all.to_dict("records"),
            "combined_reduction":    combined_reduction,
            "risk_if_gov_only":      base_prob * (1 - public_plan["risk_reduction"]),
            "risk_if_personal_only": base_prob * (1 - personal_plan["risk_reduction"]),
            "risk_if_combined":      base_prob * (1 - combined_reduction),
        }

    def _solve_single(self, actions_df, budget, mc_samples):
        if actions_df.empty or budget <= 0:
            return {"selected": [], "total_cost": 0, "risk_reduction": 0}

        best = {"selected": [], "total_cost": 0, "risk_reduction": 0}

        for r in range(1, len(actions_df) + 1):
            for combo in combinations(actions_df.itertuples(), r):
                total_cost = sum(a.cost_usd for a in combo)
                if total_cost > budget:
                    continue
                combined_efficacy = 1 - np.prod([1 - a.efficacy for a in combo])
                expected_reduction = np.mean(mc_samples) * combined_efficacy
                if expected_reduction > best["risk_reduction"]:
                    best = {
                        "selected":           [a.name_en for a in combo],
                        "descriptions":       [a.description for a in combo],
                        "total_cost":         total_cost,
                        "risk_reduction":     combined_efficacy,
                        "expected_reduction": expected_reduction,
                    }
        return best