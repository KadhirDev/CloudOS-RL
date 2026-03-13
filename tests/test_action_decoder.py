"""
Tests for ai_engine/environment/action_decoder.py
Verifies MultiDiscrete action → CloudDecision mapping.
"""

import sys
import unittest
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# MultiDiscrete action space: [cloud(4), region(10), instance(10), scaling(4), purchase(6), sla(6)]
_N_CLOUDS    = 4
_N_REGIONS   = 10
_N_INSTANCES = 10
_N_SCALING   = 4
_N_PURCHASE  = 6
_N_SLA       = 6


class TestActionDecoder(unittest.TestCase):

    def setUp(self):
        from ai_engine.environment.action_decoder import ActionDecoder
        self.decoder = ActionDecoder()

    def test_decode_returns_dict(self):
        action = np.array([0, 0, 2, 1, 1, 2])
        result = self.decoder.decode(action)
        self.assertIsInstance(result, dict)

    def test_decode_has_required_keys(self):
        action   = np.array([0, 0, 0, 0, 0, 0])
        result   = self.decoder.decode(action)
        required = ["cloud", "region", "instance_type", "purchase_option", "sla_tier"]
        for key in required:
            self.assertIn(key, result, f"Missing key: {key}")

    def test_cloud_index_maps_to_string(self):
        """Each cloud index must map to a valid cloud provider string."""
        valid_clouds = {"aws", "gcp", "azure", "hybrid"}
        for i in range(_N_CLOUDS):
            action = np.array([i, 0, 0, 0, 0, 0])
            result = self.decoder.decode(action)
            self.assertIn(result["cloud"], valid_clouds,
                          f"Cloud index {i} mapped to invalid: {result['cloud']}")

    def test_region_index_maps_to_string(self):
        """Each region index must map to a non-empty region string."""
        for i in range(_N_REGIONS):
            action = np.array([0, i, 0, 0, 0, 0])
            result = self.decoder.decode(action)
            self.assertIsInstance(result["region"], str)
            self.assertGreater(len(result["region"]), 3)

    def test_purchase_option_maps_to_valid_value(self):
        valid_purchases = {"on_demand", "spot", "reserved_1yr", "reserved_3yr", "savings_plan", "preemptible"}
        for i in range(_N_PURCHASE):
            action = np.array([0, 0, 0, 0, i, 0])
            result = self.decoder.decode(action)
            self.assertIn(result["purchase_option"], valid_purchases,
                          f"Purchase index {i} → invalid: {result['purchase_option']}")

    def test_all_zeros_action(self):
        """All-zero action must decode without error."""
        action = np.zeros(6, dtype=np.int32)
        result = self.decoder.decode(action)
        self.assertIsNotNone(result)

    def test_max_action(self):
        """Max action indices must decode without error."""
        action = np.array([
            _N_CLOUDS-1, _N_REGIONS-1, _N_INSTANCES-1,
            _N_SCALING-1, _N_PURCHASE-1, _N_SLA-1,
        ])
        result = self.decoder.decode(action)
        self.assertIsNotNone(result)

    def test_decode_is_deterministic(self):
        """Same action always produces same result."""
        action = np.array([1, 3, 2, 0, 1, 2])
        r1 = self.decoder.decode(action)
        r2 = self.decoder.decode(action)
        self.assertEqual(r1, r2)

    def test_different_actions_produce_different_results(self):
        r1 = self.decoder.decode(np.array([0, 0, 0, 0, 0, 0]))
        r2 = self.decoder.decode(np.array([1, 5, 3, 2, 2, 3]))
        # At least one field must differ
        self.assertFalse(
            all(r1.get(k) == r2.get(k) for k in ["cloud", "region", "purchase_option"]),
            "Different actions produced identical decode results"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)