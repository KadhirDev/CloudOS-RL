"""
Tests for ai_engine/environment/reward.py
Verifies the reward function R = α·ΔCost + β·ΔLatency + γ·ΔCarbon + δ·SLA − ε·Migration
"""

import sys
import unittest
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestRewardFunction(unittest.TestCase):

    def setUp(self):
        from ai_engine.environment.reward import RewardFunction
        self.reward_fn = RewardFunction()

    def _make_state(self, overrides=None):
        base = {
            "cost_per_hr":          0.096,
            "baseline_cost_per_hr": 0.096,
            "latency_ms":           50.0,
            "baseline_latency_ms":  100.0,
            "carbon_intensity":     192.0,
            "baseline_carbon":      415.0,
            "sla_met":              True,
            "migration_occurred":   False,
        }
        if overrides:
            base.update(overrides)
        return base

    def test_returns_float(self):
        r = self.reward_fn.compute(self._make_state())
        self.assertIsInstance(float(r), float)

    def test_positive_reward_for_savings(self):
        """Cheaper + lower carbon + lower latency + SLA met → positive reward."""
        r = self.reward_fn.compute(self._make_state({
            "cost_per_hr":       0.032,   # 66% cheaper than baseline
            "carbon_intensity":   42.0,   # much cleaner
            "latency_ms":         30.0,   # faster
            "sla_met":           True,
        }))
        self.assertGreater(float(r), 0.0)

    def test_negative_penalty_for_sla_breach(self):
        """SLA breach must reduce reward."""
        r_ok   = self.reward_fn.compute(self._make_state({"sla_met": True}))
        r_fail = self.reward_fn.compute(self._make_state({"sla_met": False}))
        self.assertGreater(float(r_ok), float(r_fail))

    def test_migration_penalty(self):
        """Migration adds a negative component."""
        r_no_mig = self.reward_fn.compute(self._make_state({"migration_occurred": False}))
        r_mig    = self.reward_fn.compute(self._make_state({"migration_occurred": True}))
        self.assertGreater(float(r_no_mig), float(r_mig))

    def test_expensive_cloud_reduces_reward(self):
        """Higher cost than baseline must reduce reward."""
        r_cheap = self.reward_fn.compute(self._make_state({"cost_per_hr": 0.032}))
        r_pricey= self.reward_fn.compute(self._make_state({"cost_per_hr": 0.500}))
        self.assertGreater(float(r_cheap), float(r_pricey))

    def test_high_carbon_reduces_reward(self):
        r_clean = self.reward_fn.compute(self._make_state({"carbon_intensity":  42.0}))
        r_dirty = self.reward_fn.compute(self._make_state({"carbon_intensity": 700.0}))
        self.assertGreater(float(r_clean), float(r_dirty))

    def test_reward_is_finite(self):
        """Reward must always be a finite number."""
        r = self.reward_fn.compute(self._make_state())
        self.assertTrue(np.isfinite(float(r)))

    def test_weights_sum_to_one(self):
        """Alpha + beta + gamma + delta must sum to 1.0 (ε is separate penalty)."""
        w = self.reward_fn.weights
        core = w.get("alpha", 0) + w.get("beta", 0) + w.get("gamma", 0) + w.get("delta", 0)
        self.assertAlmostEqual(core, 1.0, places=5,
                               msg=f"Reward weights α+β+γ+δ = {core:.5f} ≠ 1.0")


if __name__ == "__main__":
    unittest.main(verbosity=2)