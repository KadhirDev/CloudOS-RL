import numpy as np
from typing import Dict, Tuple

from ai_engine.environment.action_decoder import ActionDecoder


_REGION_IDX: Dict[str, int] = {r: i for i, r in enumerate(ActionDecoder.REGIONS)}

BASELINE_COST_PER_HR  = 0.096   # m5.large on-demand us-east-1
BASELINE_LATENCY_MS   = 200.0
BASELINE_CARBON_GCO2  = 41.5    # 415 gCO2/kWh × 0.1 kW


class RewardFunction:
    """
    R = α·ΔCost + β·ΔLatency + γ·ΔCarbon + δ·SLA − ε·Migration
    All sub-rewards normalised to [-1, 1] before weighting.
    Final reward clipped to [-10, 10].
    """

    DEFAULT_WEIGHTS = dict(alpha=0.35, beta=0.25, gamma=0.20, delta=0.15, epsilon=0.05)

    _PURCHASE_MULTIPLIER: Dict[str, float] = {
        "on_demand":    1.00,
        "spot":         0.33,
        "preemptible":  0.30,
        "savings_plan": 0.55,
        "reserved_1yr": 0.60,
        "reserved_3yr": 0.40,
    }

    def __init__(self, config: Dict):
        w = {**self.DEFAULT_WEIGHTS, **config.get("reward_weights", {})}
        self.alpha   = w["alpha"]
        self.beta    = w["beta"]
        self.gamma   = w["gamma"]
        self.delta   = w["delta"]
        self.epsilon = w["epsilon"]

    def compute(
        self,
        action:  Dict,
        state:   np.ndarray,
        pricing: Dict,
    ) -> Tuple[float, Dict]:

        cost_r      = self._cost(action, pricing)
        latency_r   = self._latency(action, state)
        carbon_r    = self._carbon(action, state)
        sla_r       = self._sla(action, state)
        migration_p = self._migration(action)

        total = (
            self.alpha   * cost_r
            + self.beta  * latency_r
            + self.gamma * carbon_r
            + self.delta * sla_r
            - self.epsilon * migration_p
        )
        total = float(np.clip(total, -10.0, 10.0))

        components = {
            "cost": cost_r, "latency": latency_r,
            "carbon": carbon_r, "sla": sla_r,
            "migration": migration_p, "total": total,
        }
        return total, components

    # ── sub-rewards ────────────────────────────────────────────────────────

    def _cost(self, action: Dict, pricing: Dict) -> float:
        region   = action["generic_region"]          # always use generic key
        instance = action["instance_type"]
        base     = pricing.get(region, {}).get(instance, BASELINE_COST_PER_HR)
        actual   = base * self._PURCHASE_MULTIPLIER.get(action["purchase_option"], 1.0)
        delta    = (BASELINE_COST_PER_HR - actual) / BASELINE_COST_PER_HR
        return float(np.clip(delta * 2.0, -1.0, 1.0))

    def _latency(self, action: Dict, state: np.ndarray) -> float:
        idx            = _REGION_IDX.get(action["generic_region"], 0)
        est_latency    = state[30 + idx] * 1000.0   # denormalise
        sla_latency    = state[7]         * 1000.0
        if est_latency <= sla_latency:
            return float(np.clip((BASELINE_LATENCY_MS - est_latency) / BASELINE_LATENCY_MS, 0.0, 1.0))
        return float(np.clip(-((est_latency - sla_latency) / sla_latency), -1.0, 0.0))

    def _carbon(self, action: Dict, state: np.ndarray) -> float:
        idx         = _REGION_IDX.get(action["generic_region"], 0)
        carbon_khw  = state[20 + idx] * 600.0       # gCO2/kWh
        carbon_hr   = carbon_khw * 0.1              # assume 100W workload
        delta       = (BASELINE_CARBON_GCO2 - carbon_hr) / BASELINE_CARBON_GCO2
        return float(np.clip(delta, -1.0, 1.0))

    def _sla(self, action: Dict, state: np.ndarray) -> float:
        required_tier = state[6] * 4.0              # denormalise priority → tier proxy
        assigned_tier = float(action["sla_tier"])
        if assigned_tier >= required_tier:
            return 0.5
        return float(np.clip(-0.5 * (required_tier - assigned_tier), -1.0, 0.0))

    def _migration(self, action: Dict) -> float:
        return 0.3 if action.get("requires_migration", False) else 0.0