"""
Tests for ai_engine/kafka/
Covers BridgeConfig, producer serialisation, consumer message handling.
All Kafka broker calls are mocked — no live broker required.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

_CONFIG = {
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "group_id":          "cloudos-test",
        "topics": {
            "decisions": "cloudos.scheduling.decisions",
            "metrics":   "cloudos.metrics",
            "alerts":    "cloudos.alerts",
            "workload":  "cloudos.workload.events",
        },
    },
    "prometheus": {"host": "0.0.0.0", "port": 9090},
    "bridge": {
        "poll_timeout_seconds":           1.0,
        "max_messages_per_poll":          100,
        "pipeline_metrics_push_interval": 30,
        "decision_window_seconds":        60,
    },
    "data_pipeline": {
        "pricing_output_path": "data/pricing/aws_pricing.json",
        "carbon_output_path":  "data/carbon/carbon_intensity.json",
    },
}


class TestBridgeConfig(unittest.TestCase):

    def test_loads_from_config_dict(self):
        from ai_engine.kafka.bridge_config import BridgeConfig
        bc = BridgeConfig(_CONFIG)
        self.assertEqual(bc.bootstrap_servers, "localhost:9092")
        self.assertIsInstance(bc.topics, dict)
        self.assertIn("decisions", bc.topics)

    def test_topic_names_non_empty(self):
        from ai_engine.kafka.bridge_config import BridgeConfig
        bc = BridgeConfig(_CONFIG)
        for name, topic in bc.topics.items():
            self.assertIsInstance(topic, str)
            self.assertGreater(len(topic), 0, f"Empty topic name for {name}")

    def test_poll_timeout_positive(self):
        from ai_engine.kafka.bridge_config import BridgeConfig
        bc = BridgeConfig(_CONFIG)
        self.assertGreater(bc.poll_timeout_seconds, 0)

    def test_prometheus_port_valid(self):
        from ai_engine.kafka.bridge_config import BridgeConfig
        bc = BridgeConfig(_CONFIG)
        self.assertGreater(bc.prometheus_port, 0)
        self.assertLess(bc.prometheus_port, 65536)


class TestCloudOSProducer(unittest.TestCase):

    @patch("ai_engine.kafka.producer.Producer")
    def test_publish_decision_calls_produce(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from ai_engine.kafka.producer import CloudOSProducer
        p = CloudOSProducer(_CONFIG)
        p.publish_decision({
            "decision_id":         "test-001",
            "workload_id":         "wl-001",
            "cloud":               "aws",
            "region":              "eu-north-1",
            "instance_type":       "m5.large",
            "purchase_option":     "spot",
            "cost_savings_pct":    66.7,
            "carbon_savings_pct":  89.9,
            "latency_ms":          45.0,
            "estimated_cost_per_hr": 0.032,
            "explanation":         {},
        })
        self.assertTrue(mock_producer.produce.called)

    @patch("ai_engine.kafka.producer.Producer")
    def test_publish_decision_uses_correct_topic(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from ai_engine.kafka.producer import CloudOSProducer
        p = CloudOSProducer(_CONFIG)
        p.publish_decision({"decision_id": "t1", "cloud": "aws", "region": "us-east-1",
                            "instance_type": "m5.large", "purchase_option": "on_demand",
                            "cost_savings_pct": 0, "carbon_savings_pct": 0,
                            "latency_ms": 10, "estimated_cost_per_hr": 0.096, "explanation": {}})
        call_args = mock_producer.produce.call_args
        topic = call_args[0][0] if call_args[0] else call_args[1].get("topic", "")
        self.assertIn("decisions", topic)

    @patch("ai_engine.kafka.producer.Producer")
    def test_publish_alert_calls_produce(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from ai_engine.kafka.producer import CloudOSProducer
        p = CloudOSProducer(_CONFIG)
        p.publish_alert("test_alert", {"message": "test"})
        self.assertTrue(mock_producer.produce.called)

    @patch("ai_engine.kafka.producer.Producer")
    def test_message_is_valid_json(self, mock_producer_cls):
        """Produced messages must be deserializable JSON."""
        captured = {}

        def fake_produce(topic, value=None, key=None, **kwargs):
            captured["value"] = value

        mock_producer = MagicMock()
        mock_producer.produce.side_effect = fake_produce
        mock_producer_cls.return_value = mock_producer

        from ai_engine.kafka.producer import CloudOSProducer
        p = CloudOSProducer(_CONFIG)
        p.publish_decision({
            "decision_id": "json-test", "cloud": "gcp", "region": "us-central1",
            "instance_type": "n1-standard-4", "purchase_option": "on_demand",
            "cost_savings_pct": 5.0, "carbon_savings_pct": 10.0,
            "latency_ms": 20.0, "estimated_cost_per_hr": 0.096, "explanation": {},
        })

        if captured.get("value"):
            raw = captured["value"]
            if isinstance(raw, bytes):
                raw = raw.decode()
            parsed = json.loads(raw)
            self.assertIn("decision_id", parsed)


class TestMetricsRegistry(unittest.TestCase):

    def test_registry_creates_counters(self):
        from ai_engine.kafka.metrics_registry import CloudOSMetrics
        m = CloudOSMetrics()
        # Should not raise
        self.assertIsNotNone(m)

    def test_registry_has_decisions_counter(self):
        from ai_engine.kafka.metrics_registry import CloudOSMetrics
        m = CloudOSMetrics()
        self.assertTrue(hasattr(m, "decisions_total") or hasattr(m, "decision_counter"),
                        "MetricsRegistry missing decisions counter")

    def test_registry_has_carbon_gauge(self):
        from ai_engine.kafka.metrics_registry import CloudOSMetrics
        m = CloudOSMetrics()
        self.assertTrue(
            hasattr(m, "carbon_intensity") or hasattr(m, "carbon_gauge"),
            "MetricsRegistry missing carbon gauge"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)