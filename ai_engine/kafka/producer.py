"""
CloudOS-RL Kafka Producer
==========================
Publishes messages to four Kafka topics.

Topics:
  cloudos.scheduling.decisions  <- SchedulingDecision objects from API
  cloudos.metrics               <- Pipeline health metrics
  cloudos.alerts                <- Cost anomaly and system alerts
  cloudos.workload.events       <- Workload lifecycle events

Compatible with:
  - Module D bridge consumer (kafka_prometheus_bridge.py reads these)
  - Module G pipeline orchestrator (publishes its metrics via this)
  - Backend API (scheduling.py calls publish_decision)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    raise ImportError(
        "\n\nconfluent-kafka is not installed.\n"
        "Fix: run   pip install confluent-kafka   then try again.\n"
    )

logger = logging.getLogger(__name__)

# Topic names — shared with kafka_prometheus_bridge.py
TOPICS = {
    "decisions": "cloudos.scheduling.decisions",
    "metrics": "cloudos.metrics",
    "alerts": "cloudos.alerts",
    "workloads": "cloudos.workload.events",
}


class CloudOSProducer:
    """
    Thread-safe Kafka producer for CloudOS-RL.
    Creates topics on first use if they do not exist.
    Kafka failures are non-fatal and must not break API inference flow.
    """

    def __init__(self, config: Dict):
        config = config or {}
        self._config = config

        kafka_cfg = config.get("kafka", {}) or {}

        # Env var is highest priority — injected by Kubernetes ConfigMap
        env_bootstrap = os.environ.get("CLOUDOS_KAFKA_BOOTSTRAP", "").strip()
        cfg_bootstrap = str(kafka_cfg.get("bootstrap_servers", "") or "").strip()

        bootstrap = env_bootstrap or cfg_bootstrap or "host.minikube.internal:9092"

        # Safety: never allow localhost fallback inside k8s/runtime path
        if bootstrap.startswith("localhost"):
            if env_bootstrap and not env_bootstrap.startswith("localhost"):
                bootstrap = env_bootstrap
            else:
                bootstrap = "host.minikube.internal:9092"
                logger.warning(
                    "CloudOSProducer: bootstrap was localhost — overriding to %s",
                    bootstrap,
                )

        self._servers = bootstrap
        self._topics = dict(TOPICS)

        parts = int(kafka_cfg.get("partitions", 3))
        rep = int(kafka_cfg.get("replication", 1))

        self._partitions = parts
        self._replication = rep
        self._producer: Optional[Producer] = None

        logger.info("CloudOSProducer: bootstrap_servers=%s", self._servers)

        self._connect()
        self._ensure_topics(self._servers, self._partitions, self._replication)

    # -----------------------------------------------------------------------
    # Public: publish methods
    # -----------------------------------------------------------------------

    def publish_decision(self, decision: Dict) -> bool:
        """
        Publishes a scheduling decision to cloudos.scheduling.decisions.
        Expected keys: decision_id, workload_id, cloud, region, instance_type,
                       purchase_option, cost_savings_pct, carbon_savings_pct,
                       latency_ms, estimated_cost_per_hr, explanation,
                       actual_reward (optional)

        Fire-and-forget. Never raises. Returns False if Kafka unavailable.
        """
        key = decision.get("decision_id", str(int(time.time() * 1000)))

        payload = {
            "decision_id": decision.get("decision_id", key),
            "workload_id": decision.get("workload_id"),
            "cloud": decision.get("cloud"),
            "region": decision.get("region"),
            "instance_type": decision.get("instance_type"),
            "purchase_option": decision.get("purchase_option"),
            "sla_tier": decision.get("sla_tier"),
            "estimated_cost_per_hr": decision.get("estimated_cost_per_hr"),
            "cost_savings_pct": decision.get("cost_savings_pct"),
            "carbon_savings_pct": decision.get("carbon_savings_pct"),
            "latency_ms": decision.get("latency_ms"),
            "workload_type": decision.get("workload_type"),
            "explanation": decision.get("explanation", {}),
            "actual_reward": decision.get("actual_reward", None),
            "ts": time.time(),
            "ts_iso": _now_iso(),
        }

        return self._send(self._topics["decisions"], key, payload)

    def publish_metrics(self, metrics: Dict) -> bool:
        """
        Publishes pipeline or system metrics to cloudos.metrics.
        Bridge reads this to update Prometheus pipeline health gauges.
        Expected keys: pricing_fetches, carbon_fetches, cur_fetches,
                       pricing_errors, carbon_errors, cur_errors
        """
        key = str(int(time.time() * 1000))
        return self._send(self._topics["metrics"], key, {**metrics, "_ts": time.time()})

    def publish_alert(self, kind: str, detail: Dict) -> bool:
        """
        Publishes an alert to cloudos.alerts.
        kind: e.g. "cost_anomaly", "sla_breach", "spot_interruption"

        Fire-and-forget. Never raises. Returns False if Kafka unavailable.
        """
        payload = {
            "kind": kind,
            "detail": detail,
            "ts": time.time(),
            "ts_iso": _now_iso(),
        }
        return self._send(self._topics["alerts"], kind, payload)

    def publish_workload_event(
        self,
        workload_id: str,
        workload_type: str,
        event_type: str,
        detail: Optional[Dict] = None,
    ) -> bool:
        """
        Publishes a workload lifecycle event to cloudos.workload.events.
        event_type: e.g. "submitted", "scheduled", "completed", "failed"
        """
        payload = {
            "workload_id": workload_id,
            "workload_type": workload_type,
            "event_type": event_type,
            "detail": detail or {},
            "ts": time.time(),
            "ts_iso": _now_iso(),
        }
        return self._send(self._topics["workloads"], workload_id, payload)

    def flush(self, timeout: float = 5.0):
        """Blocks until all buffered messages are delivered."""
        if self._producer is not None:
            try:
                remaining = self._producer.flush(timeout=timeout)
                if remaining > 0:
                    logger.warning(
                        "Kafka flush: %d messages not delivered within %.1fs",
                        remaining,
                        timeout,
                    )
            except Exception as exc:
                logger.warning("Kafka flush failed (non-fatal): %s", exc)

    # -----------------------------------------------------------------------
    # Private
    # -----------------------------------------------------------------------

    def _connect(self):
        """Non-blocking connect — failure is logged but does NOT raise."""
        try:
            self._producer = Producer(
                {
                    "bootstrap.servers": self._servers,
                    "client.id": "cloudos-producer",
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 500,
                    "compression.type": "lz4",
                    "linger.ms": 5,
                    "message.max.bytes": 1_048_576,
                    "socket.timeout.ms": 5000,
                    "message.timeout.ms": 10000,
                    "socket.connection.setup.timeout.ms": 5000,
                }
            )
            logger.info("CloudOSProducer: connected to %s", self._servers)
        except Exception as exc:
            self._producer = None
            logger.warning(
                "CloudOSProducer: connection failed (%s) — publish calls will be no-ops",
                exc,
            )

    def _send(self, topic: str, key: str, payload: Dict) -> bool:
        """
        Internal fire-and-forget send.
        Never raises. Returns False if Kafka is unavailable or produce fails.
        """
        if self._producer is None:
            logger.debug(
                "Kafka unavailable — skipping publish to topic=%s key=%s",
                topic,
                key,
            )
            return False

        try:
            self._producer.produce(
                topic=topic,
                key=str(key).encode("utf-8"),
                value=json.dumps(payload, default=str).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)  # non-blocking — triggers callbacks only
            return True

        except BufferError:
            logger.warning("Kafka producer buffer full — flushing ...")
            try:
                self._producer.flush(5.0)
            except Exception as exc:
                logger.warning("Kafka flush after buffer-full failed: %s", exc)
            return False

        except KafkaException as exc:
            logger.warning(
                "Kafka produce failed (non-fatal) [%s] key=%s: %s",
                topic,
                key,
                exc,
            )
            return False

        except Exception as exc:
            logger.warning(
                "Kafka produce unexpected failure (non-fatal) [%s] key=%s: %s",
                topic,
                key,
                exc,
            )
            return False

    @staticmethod
    def _on_delivery(err: Any, msg: Any):
        if err:
            logger.warning("Kafka delivery failed: topic=%s err=%s", msg.topic(), err)
        else:
            logger.debug(
                "Delivered → %s [partition=%d offset=%d]",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def _ensure_topics(self, servers: str, partitions: int, replication: int):
        """Creates any missing Kafka topics. Non-fatal if Kafka is unavailable."""
        try:
            admin = AdminClient({"bootstrap.servers": servers})
            existing = set(admin.list_topics(timeout=10).topics)

            to_create: List[NewTopic] = [
                NewTopic(
                    topic_name,
                    num_partitions=partitions,
                    replication_factor=replication,
                )
                for topic_name in self._topics.values()
                if topic_name not in existing
            ]

            if not to_create:
                logger.debug("All Kafka topics already exist.")
                return

            futures = admin.create_topics(to_create)
            for topic_name, future in futures.items():
                try:
                    future.result()
                    logger.info("Created Kafka topic: %s", topic_name)
                except Exception as exc:
                    if "already exists" not in str(exc).lower():
                        logger.warning("Topic creation [%s]: %s", topic_name, exc)

        except KafkaException as exc:
            logger.warning(
                "Cannot connect to Kafka at %s: %s\n"
                "Kafka publishing will remain non-fatal/no-op until Kafka is reachable.",
                servers,
                exc,
            )
        except Exception as exc:
            logger.warning(
                "Kafka topic initialization failed at %s (non-fatal): %s",
                servers,
                exc,
            )


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()