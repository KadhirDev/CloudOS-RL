"""
Tests for ai_engine/environment/state_builder.py
Verifies the 45-dim state vector construction.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestStateBuilder(unittest.TestCase):

    def setUp(self):
        self.config = {
            "data_pipeline": {
                "pricing_output_path": "data/pricing/aws_pricing.json",
                "carbon_output_path":  "data/carbon/carbon_intensity.json",
            }
        }
        self.workload = {
            "cpu_request_vcpu":        4.0,
            "memory_request_gb":       8.0,
            "gpu_count":               0,
            "storage_gb":              100.0,
            "network_bandwidth_gbps":  1.0,
            "expected_duration_hours": 2.0,
            "priority":                2,
            "sla_latency_ms":          200,
            "workload_type_encoded":   0,
            "is_spot_tolerant":        1,
        }
        self.pricing = {
            r: 0.096 + i * 0.005 for i, r in enumerate([
                "us-east-1","us-west-2","eu-west-1","eu-central-1",
                "ap-southeast-1","ap-northeast-1","us-central1",
                "europe-west4","eastus","westeurope",
            ])
        }
        self.carbon = {
            r: 200.0 + i * 30 for i, r in enumerate([
                "us-east-1","us-west-2","eu-west-1","eu-central-1",
                "ap-southeast-1","ap-northeast-1","us-central1",
                "europe-west4","eastus","westeurope",
            ])
        }

    def _make_builder(self):
        from ai_engine.environment.state_builder import StateBuilder
        return StateBuilder(self.config)

    def test_output_shape(self):
        """State vector must be exactly (45,)."""
        b = self._make_builder()
        s = b.build(self.workload, self.pricing, self.carbon, [])
        self.assertEqual(s.shape, (45,))

    def test_output_dtype(self):
        """State vector must be float32."""
        b = self._make_builder()
        s = b.build(self.workload, self.pricing, self.carbon, [])
        self.assertEqual(s.dtype, np.float32)

    def test_no_nan(self):
        b = self._make_builder()
        s = b.build(self.workload, self.pricing, self.carbon, [])
        self.assertFalse(np.any(np.isnan(s)), "State contains NaN")

    def test_no_inf(self):
        b = self._make_builder()
        s = b.build(self.workload, self.pricing, self.carbon, [])
        self.assertFalse(np.any(np.isinf(s)), "State contains Inf")

    def test_values_normalised(self):
        """Most values should be in [0, 1] after normalisation."""
        b = self._make_builder()
        s = b.build(self.workload, self.pricing, self.carbon, [])
        # At least 70% of values should be in [0, 1]
        in_range = np.sum((s >= 0.0) & (s <= 1.0))
        self.assertGreater(in_range / 45, 0.7)

    def test_different_workloads_differ(self):
        """Two different workloads must produce different state vectors."""
        b  = self._make_builder()
        w2 = {**self.workload, "cpu_request_vcpu": 16.0, "gpu_count": 4}
        s1 = b.build(self.workload, self.pricing, self.carbon, [])
        s2 = b.build(w2,           self.pricing, self.carbon, [])
        self.assertFalse(np.allclose(s1, s2), "Different workloads produced identical states")

    def test_empty_pricing_uses_defaults(self):
        """StateBuilder must not crash with empty pricing dict."""
        b = self._make_builder()
        s = b.build(self.workload, {}, self.carbon, [])
        self.assertEqual(s.shape, (45,))
        self.assertFalse(np.any(np.isnan(s)))

    def test_empty_carbon_uses_defaults(self):
        b = self._make_builder()
        s = b.build(self.workload, self.pricing, {}, [])
        self.assertEqual(s.shape, (45,))
        self.assertFalse(np.any(np.isnan(s)))

    def test_history_affects_last_5_dims(self):
        """Non-empty history should affect dims [40:45]."""
        b  = self._make_builder()
        s0 = b.build(self.workload, self.pricing, self.carbon, [])
        s1 = b.build(self.workload, self.pricing, self.carbon,
                     [{"reward": 3.5, "cost_savings": 0.3, "carbon_savings": 0.2}])
        # The last 5 dims should differ when history is provided
        # (they should not ALL be identical)
        self.assertFalse(np.allclose(s0[40:], s1[40:]),
                         "History had no effect on history dims [40:45]")


if __name__ == "__main__":
    unittest.main(verbosity=2)