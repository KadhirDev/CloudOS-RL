"""
Tests for ai_engine/data_pipeline/
Covers DataNormalizer + BackgroundDataGenerator integration with pipeline files.
All AWS API and Electricity Maps calls are mocked — no network required.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDataNormalizer(unittest.TestCase):
    """Tests for data_normalizer.py — atomic writes + fallback values."""

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def _make_config(self):
        return {
            "data_pipeline": {
                "pricing_output_path":      str(self.tmpdir / "aws_pricing.json"),
                "actual_costs_output_path": str(self.tmpdir / "aws_actual_costs.json"),
                "carbon_output_path":       str(self.tmpdir / "carbon_intensity.json"),
            }
        }

    def test_write_pricing_creates_file(self):
        from ai_engine.data_pipeline.data_normalizer import DataNormalizer
        norm   = DataNormalizer(self._make_config())
        sample = {"us-east-1": {"on_demand_per_vcpu_hr": 0.096}}
        norm.write_pricing(sample)
        path = self.tmpdir / "aws_pricing.json"
        self.assertTrue(path.exists())

    def test_write_pricing_valid_json(self):
        from ai_engine.data_pipeline.data_normalizer import DataNormalizer
        norm   = DataNormalizer(self._make_config())
        sample = {"us-east-1": {"on_demand_per_vcpu_hr": 0.096},
                  "eu-north-1": {"on_demand_per_vcpu_hr": 0.098}}
        norm.write_pricing(sample)
        with open(self.tmpdir / "aws_pricing.json") as f:
            data = json.load(f)
        self.assertIn("us-east-1", data)

    def test_write_carbon_creates_file(self):
        from ai_engine.data_pipeline.data_normalizer import DataNormalizer
        norm   = DataNormalizer(self._make_config())
        sample = {"us-east-1": {"gco2_per_kwh": 415.0}}
        norm.write_carbon(sample)
        path = self.tmpdir / "carbon_intensity.json"
        self.assertTrue(path.exists())

    def test_write_never_creates_empty_file(self):
        """Normalizer must write fallback values, never an empty {}."""
        from ai_engine.data_pipeline.data_normalizer import DataNormalizer
        norm = DataNormalizer(self._make_config())
        norm.write_pricing({})   # empty input → should write fallback
        with open(self.tmpdir / "aws_pricing.json") as f:
            data = json.load(f)
        self.assertGreater(len(data), 0, "Empty pricing file written — should contain fallback data")

    def test_atomic_write_does_not_leave_tmp_file(self):
        """After write_pricing, no .tmp.json file should remain."""
        from ai_engine.data_pipeline.data_normalizer import DataNormalizer
        norm = DataNormalizer(self._make_config())
        norm.write_pricing({"us-east-1": {"on_demand_per_vcpu_hr": 0.096}})
        tmp_files = list(self.tmpdir.glob("*.tmp.json"))
        self.assertEqual(len(tmp_files), 0, f"Temp files remain: {tmp_files}")


class TestCarbonAPIClient(unittest.TestCase):
    """Tests for carbon_api_client.py — static fallback path."""

    def setUp(self):
        self.config = {
            "data_pipeline": {
                "carbon_output_path": "data/carbon/carbon_intensity.json",
            }
        }

    def test_static_fallback_returns_dict(self):
        from ai_engine.data_pipeline.carbon_api_client import CarbonAPIClient
        client = CarbonAPIClient(self.config)
        data   = client._static_fallback()
        self.assertIsInstance(data, dict)
        self.assertGreater(len(data), 0)

    def test_static_fallback_has_16_regions(self):
        from ai_engine.data_pipeline.carbon_api_client import CarbonAPIClient
        client = CarbonAPIClient(self.config)
        data   = client._static_fallback()
        self.assertEqual(len(data), 16, f"Expected 16 regions, got {len(data)}")

    def test_static_fallback_all_positive_co2(self):
        from ai_engine.data_pipeline.carbon_api_client import CarbonAPIClient
        client = CarbonAPIClient(self.config)
        data   = client._static_fallback()
        for region, entry in data.items():
            co2 = entry.get("gco2_per_kwh", 0)
            self.assertGreater(co2, 0, f"Non-positive CO2 for {region}")

    def test_eu_north_1_cleanest(self):
        """eu-north-1 should be the cleanest (42 gCO2/kWh)."""
        from ai_engine.data_pipeline.carbon_api_client import CarbonAPIClient
        client = CarbonAPIClient(self.config)
        data   = client._static_fallback()
        co2_values = {r: e.get("gco2_per_kwh", 999) for r, e in data.items()}
        cleanest   = min(co2_values, key=co2_values.get)
        self.assertEqual(cleanest, "eu-north-1",
                         f"Expected eu-north-1 to be cleanest, got {cleanest} ({co2_values[cleanest]})")

    @patch("ai_engine.data_pipeline.carbon_api_client.httpx")
    def test_fetch_uses_fallback_on_api_error(self, mock_httpx):
        """If API key missing or call fails, should return static fallback."""
        mock_httpx.get.side_effect = Exception("No API key")
        from ai_engine.data_pipeline.carbon_api_client import CarbonAPIClient
        client = CarbonAPIClient(self.config)
        data   = client.fetch()
        self.assertIsInstance(data, dict)
        self.assertGreater(len(data), 0)


class TestPipelineOrchestrator(unittest.TestCase):
    """Tests for pipeline_orchestrator.py — integration with mocked AWS APIs."""

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.config = {
            "data_pipeline": {
                "pricing_refresh_sec":      3600,
                "carbon_refresh_sec":       900,
                "cur_refresh_sec":          3600,
                "pricing_output_path":      str(self.tmpdir / "aws_pricing.json"),
                "actual_costs_output_path": str(self.tmpdir / "aws_actual_costs.json"),
                "carbon_output_path":       str(self.tmpdir / "carbon_intensity.json"),
                "anomaly_threshold_pct":    50.0,
            }
        }

    @patch("ai_engine.data_pipeline.aws_pricing_fetcher.boto3")
    @patch("ai_engine.data_pipeline.aws_cur_ingestor.boto3")
    @patch("ai_engine.data_pipeline.carbon_api_client.httpx")
    def test_run_once_creates_all_output_files(
        self, mock_httpx, mock_boto3_cur, mock_boto3_pricing
    ):
        """Pipeline run must create all 3 output files."""
        # Mock pricing API
        mock_pricing_client = MagicMock()
        mock_pricing_client.get_products.return_value = {"PriceList": []}
        mock_boto3_pricing.client.return_value = mock_pricing_client

        # Mock CUR
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = {
            "ResultsByTime": [{"Groups": [], "Total": {"BlendedCost": {"Amount": "10.50"}}}]
        }
        mock_boto3_cur.client.return_value = mock_ce

        # Mock carbon API failure → static fallback used
        mock_httpx.get.side_effect = Exception("no key")

        from ai_engine.data_pipeline.pipeline_orchestrator import DataPipelineOrchestrator
        orch = DataPipelineOrchestrator(self.config)
        orch.run_once()

        self.assertTrue((self.tmpdir / "aws_pricing.json").exists(),
                        "aws_pricing.json not created")
        self.assertTrue((self.tmpdir / "carbon_intensity.json").exists(),
                        "carbon_intensity.json not created")

    def test_metrics_initialised_to_zero(self):
        from ai_engine.data_pipeline.pipeline_orchestrator import DataPipelineOrchestrator
        orch = DataPipelineOrchestrator(self.config)
        m    = orch.get_metrics()
        self.assertIsInstance(m, dict)
        for key in ["pricing_fetches", "carbon_fetches", "cur_fetches",
                    "pricing_errors", "carbon_errors", "cur_errors"]:
            self.assertIn(key, m)
            self.assertEqual(m[key], 0, f"Metric {key} not zero at init")


if __name__ == "__main__":
    unittest.main(verbosity=2)  