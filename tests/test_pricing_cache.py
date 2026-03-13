"""
Tests for ai_engine/cloud_adapter/pricing_cache.py
Verifies Module G pricing file reading + fallback chain.
"""

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestPricingCache(unittest.TestCase):

    def _make_pricing_file(self, tmpdir: Path, data: dict) -> Path:
        p = tmpdir / "aws_pricing.json"
        p.write_text(json.dumps(data, indent=2))
        return p

    def _make_config(self, pricing_path: str) -> dict:
        return {
            "data_pipeline": {
                "pricing_output_path": pricing_path,
                "carbon_output_path":  "data/carbon/carbon_intensity.json",
            }
        }

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def test_loads_from_file(self):
        """PricingCache must load prices from Module G output file."""
        data = {
            "us-east-1": {"on_demand_per_vcpu_hr": 0.096},
            "eu-north-1": {"on_demand_per_vcpu_hr": 0.098},
        }
        p      = self._make_pricing_file(self.tmpdir, data)
        config = self._make_config(str(p))

        from ai_engine.cloud_adapter.pricing_cache import PricingCache
        cache  = PricingCache(config)
        prices = cache.get_current_pricing()

        self.assertIn("us-east-1", prices)
        self.assertGreater(prices["us-east-1"], 0)

    def test_get_price_returns_float(self):
        data = {"us-east-1": {"on_demand_per_vcpu_hr": 0.096}}
        p    = self._make_pricing_file(self.tmpdir, data)
        from ai_engine.cloud_adapter.pricing_cache import PricingCache
        cache = PricingCache(self._make_config(str(p)))
        price = cache.get_price("us-east-1", "m5.large", "on_demand")
        self.assertIsInstance(price, float)
        self.assertGreater(price, 0.0)

    def test_fallback_on_missing_file(self):
        """PricingCache must return static fallback when file is absent."""
        config = self._make_config("/nonexistent/path/aws_pricing.json")
        from ai_engine.cloud_adapter.pricing_cache import PricingCache
        cache  = PricingCache(config)
        prices = cache.get_current_pricing()
        self.assertIsInstance(prices, dict)
        self.assertGreater(len(prices), 0)

    def test_fallback_on_corrupt_file(self):
        p = self.tmpdir / "aws_pricing.json"
        p.write_text("{this is not valid json}")
        from ai_engine.cloud_adapter.pricing_cache import PricingCache
        cache  = PricingCache(self._make_config(str(p)))
        prices = cache.get_current_pricing()
        self.assertIsInstance(prices, dict)
        self.assertGreater(len(prices), 0)

    def test_spot_price_less_than_on_demand(self):
        """Spot price should be less than on-demand for the same region."""
        data = {"us-east-1": {"on_demand_per_vcpu_hr": 0.096, "spot_discount": 0.70}}
        p    = self._make_pricing_file(self.tmpdir, data)
        from ai_engine.cloud_adapter.pricing_cache import PricingCache
        cache    = PricingCache(self._make_config(str(p)))
        on_demand = cache.get_price("us-east-1", "m5.large", "on_demand")
        spot      = cache.get_price("us-east-1", "m5.large", "spot")
        self.assertLess(spot, on_demand,
                        f"spot ({spot}) should be < on_demand ({on_demand})")

    def test_all_prices_positive(self):
        data = {
            "us-east-1":  {"on_demand_per_vcpu_hr": 0.096},
            "eu-north-1": {"on_demand_per_vcpu_hr": 0.098},
            "us-west-2":  {"on_demand_per_vcpu_hr": 0.096},
        }
        p = self._make_pricing_file(self.tmpdir, data)
        from ai_engine.cloud_adapter.pricing_cache import PricingCache
        cache  = PricingCache(self._make_config(str(p)))
        prices = cache.get_current_pricing()
        for region, price in prices.items():
            self.assertGreater(float(price), 0.0, f"Non-positive price for {region}")


if __name__ == "__main__":
    unittest.main(verbosity=2)