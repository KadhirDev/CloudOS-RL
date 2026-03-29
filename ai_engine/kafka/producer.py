"""
CloudOS-RL Kafka Producer
==========================

Non-blocking Kafka producer for CloudOS-RL with:
- safe bootstrap resolution for Kubernetes/Minikube
- optional/no-op behavior when Kafka is unavailable
- topic auto-creation when broker/admin client is reachable
- lightweight broker probe on startup
- one-shot reconnect before publish
- background poll/flush thread to avoid queue buildup
- delivery callbacks with actionable logging

This file is designed to be backward-compatible with the existing codebase:
- publish_decision(...)
- publish_metrics(...)
- publish_alert(...)
- publish_workload_event(...)
- flush(...)
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

TOPICS = {
    "decisions": "cloudos.scheduling.decisions",
    "metrics": "cloudos.metrics",
    "alerts": "cloudos.alerts",
    "workloads": "cloudos.workload.events",
}

_REQUIRED_DECISION_FIELDS = frozenset(
    ["decision_id", "cloud", "region", "instance_type", "purchase_option"]
)

# Backup callback servicing interval.
_FLUSH_INTERVAL_SEC = 5


class CloudOSProducer:
    """
    Thread-safe Kafka producer for CloudOS-RL.

    Goals:
    - Never break API inference flow if Kafka is unavailable
    - Be safe in Kubernetes/Minikube environments
    - Preserve existing topic names and public methods
    """

    def __init__(self, config: Optional[Dict] = None):
        config = config or {}
        self._config = config
        self._lock = threading.Lock()

        kafka_cfg = config.get("kafka", {}) or {}

        self._servers = self._resolve_bootstrap(kafka_cfg)
        self._topics = dict(TOPICS)

        self._partitions = int(kafka_cfg.get("partitions", 3))
        self._replication = int(kafka_cfg.get("replication", 1))

        self._producer = None

        logger.info("CloudOSProducer: bootstrap_servers=%s", self._servers)

        self._connect()
        self._ensure_topics()

        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            daemon=True,
            name="kafka-flush",
        )
        self._flush_thread.start()

    # ---------------------------------------------------------------------
    # Bootstrap / connection
    # ---------------------------------------------------------------------

    @staticmethod
    def _resolve_bootstrap(config: Dict[str, Any]) -> str:
        """
        Priority order:
          1. CLOUDOS_KAFKA_BOOTSTRAP env var  (from Kubernetes ConfigMap env injection)
          2. config["kafka"]["bootstrap_servers"]  (from settings.yaml ConfigMap mount)
          3. Hardcoded fallback

        Rejects localhost/127.0.0.1 — not reachable from inside Kubernetes pods.
        """
        sources = [
            (
                "env:CLOUDOS_KAFKA_BOOTSTRAP",
                os.environ.get("CLOUDOS_KAFKA_BOOTSTRAP", "").strip(),
            ),
            (
                "settings.yaml:kafka.bootstrap",
                str(config.get("bootstrap_servers", "") or "").strip(),
            ),
        ]

        for source_name, candidate in sources:
            if not candidate:
                logger.debug(
                    "KafkaProducer._resolve_bootstrap: %s is empty — skipping",
                    source_name,
                )
                continue

            lowered = candidate.lower()
            if "localhost" in lowered or "127.0.0.1" in lowered:
                logger.warning(
                    "KafkaProducer._resolve_bootstrap: %s='%s' contains localhost — "
                    "not reachable from Kubernetes pod — skipping",
                    source_name,
                    candidate,
                )
                continue

            logger.info(
                "KafkaProducer: bootstrap_servers=%s  (source: %s)",
                candidate,
                source_name,
            )
            return candidate

        fallback = "192.168.49.1:9092"
        logger.warning(
            "KafkaProducer: no valid bootstrap found in env or config — "
            "using hardcoded fallback: %s",
            fallback,
        )
        return fallback

    def _connect(self) -> None:
        """
        Create the Producer instance if possible.
        Never raises. Kafka remains optional.
        """
        try:
            from confluent_kafka import Producer
        except ImportError:
            logger.warning(
                "CloudOSProducer: confluent_kafka not installed; Kafka publish "
                "will remain disabled (non-fatal)"
            )
            self._producer = None
            return

        try:
            producer = Producer(
                {
                    "bootstrap.servers": self._servers,
                    "client.id": "cloudos-producer",
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 500,
                    "compression.type": "lz4",
                    "linger.ms": 5,
                    "message.max.bytes": 1_048_576,
                    "socket.timeout.ms": 6000,
                    "message.timeout.ms": 15000,
                    "socket.connection.setup.timeout.ms": 6000,
                    "log_level": 3,
                }
            )

            if self._probe_connection(producer):
                with self._lock:
                    self._producer = producer
                logger.info(
                    "CloudOSProducer: connected and broker reachable at %s",
                    self._servers,
                )
            else:
                with self._lock:
                    self._producer = None
                logger.warning(
                    "CloudOSProducer: broker not reachable at %s; Kafka will remain "
                    "in non-fatal no-op mode until reconnect succeeds",
                    self._servers,
                )

        except Exception as exc:
            with self._lock:
                self._producer = None
            logger.warning(
                "CloudOSProducer: connection failed (%s); Kafka will remain optional",
                exc,
            )

    @staticmethod
    def _probe_connection(producer: Any, timeout: float = 4.0) -> bool:
        """
        Lightweight broker probe using metadata fetch.
        """
        try:
            meta = producer.list_topics(timeout=timeout)
            return bool(getattr(meta, "brokers", {}))
        except Exception as exc:
            logger.debug("CloudOSProducer._probe_connection failed: %s", exc)
            return False

    def _reconnect_if_needed(self) -> bool:
        """
        Attempt one reconnect if producer is currently unavailable.
        """
        with self._lock:
            if self._producer is not None:
                return True

        logger.info("CloudOSProducer: producer unavailable; attempting reconnect")
        self._connect()

        with self._lock:
            return self._producer is not None

    # ---------------------------------------------------------------------
    # Public publish methods
    # ---------------------------------------------------------------------

    def publish_decision(self, decision: Dict[str, Any]) -> bool:
        """
        Publishes a scheduling decision to cloudos.scheduling.decisions.

        Uses flush() on the exact producer instance that executed produce(),
        which guarantees delivery callbacks are serviced before this method
        returns. This avoids callback races with the background flush thread.

        Never raises. Returns False if Kafka is unavailable or the payload is
        missing required scheduling fields.
        """
        missing = _REQUIRED_DECISION_FIELDS - set(decision.keys())
        if missing:
            logger.warning(
                "CloudOSProducer.publish_decision: missing required fields %s; skipping",
                sorted(missing),
            )
            return False

        if not self._reconnect_if_needed():
            return False

        raw_key = decision.get("decision_id")
        key_str = str(raw_key) if raw_key else str(int(time.time() * 1000))
        encoded_key = key_str.encode("utf-8") if key_str else None
        display_id = str(raw_key)[:8] if raw_key else "?"
        topic = self._topics["decisions"]

        payload = {
            "decision_id": decision.get("decision_id", key_str),
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
            "actual_reward": decision.get("actual_reward"),
            "ts": time.time(),
            "ts_iso": _now_iso(),
        }

        encoded_value = json.dumps(payload, default=str).encode("utf-8")

        logger.info(
            "KafkaProducer: sending decision_id=%s to topic=%s",
            display_id,
            topic,
        )

        try:
            with self._lock:
                producer = self._producer
                if producer is None:
                    return False

                producer.produce(
                    topic=topic,
                    key=encoded_key,
                    value=encoded_value,
                    on_delivery=self._on_delivery,
                )

            # Flush the exact producer instance used above.
            remaining = producer.flush(timeout=5.0)
            if remaining > 0:
                logger.warning(
                    "CloudOSProducer.publish_decision: flush timed out with %d "
                    "undelivered message(s) for decision=%s",
                    remaining,
                    display_id,
                )

            return True

        except BufferError:
            logger.warning(
                "CloudOSProducer.publish_decision: producer buffer full; flushing briefly"
            )
            try:
                with self._lock:
                    producer = self._producer
                if producer is not None:
                    producer.flush(5.0)
            except Exception as exc:
                logger.warning(
                    "CloudOSProducer.publish_decision: flush after buffer-full failed "
                    "(non-fatal): %s",
                    exc,
                )
            return False

        except Exception as exc:
            logger.warning(
                "CloudOSProducer.publish_decision: produce/flush failed (non-fatal) "
                "decision=%s: %s",
                display_id,
                exc,
            )
            return False

    def publish_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Publishes pipeline/system metrics to cloudos.metrics.
        """
        key = str(int(time.time() * 1000))
        payload = {**metrics, "_ts": time.time(), "_ts_iso": _now_iso()}
        return self._send(self._topics["metrics"], key, payload)

    def publish_alert(self, kind: str, detail: Dict[str, Any]) -> bool:
        """
        Publishes an alert to cloudos.alerts.
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
        detail: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publishes a workload lifecycle event to cloudos.workload.events.
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

    def flush(self, timeout: float = 5.0) -> None:
        """
        Blocking flush. Safe to call at graceful shutdown.
        """
        with self._lock:
            producer = self._producer

        if producer is None:
            return

        try:
            remaining = producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(
                    "CloudOSProducer.flush: %d messages not delivered within %.1fs",
                    remaining,
                    timeout,
                )
        except Exception as exc:
            logger.warning("CloudOSProducer.flush failed (non-fatal): %s", exc)

    # ---------------------------------------------------------------------
    # Internal send / topic init
    # ---------------------------------------------------------------------

    def _send(
        self,
        topic: str,
        key: Optional[str],
        payload: Dict[str, Any],
        callback_poll_timeout: float = 0.0,
    ) -> bool:
        """
        Internal fire-and-forget send.
        Never raises. Returns False if Kafka is unavailable or produce fails.

        Important: callback servicing is always performed on the exact producer
        instance that executed produce(), avoiding races if self._producer is
        replaced concurrently.
        """
        if not self._reconnect_if_needed():
            logger.debug(
                "CloudOSProducer: Kafka unavailable; skipping publish topic=%s key=%s",
                topic,
                key,
            )
            return False

        try:
            encoded_key = str(key).encode("utf-8") if key else None
            encoded_value = json.dumps(payload, default=str).encode("utf-8")

            with self._lock:
                producer = self._producer
                if producer is None:
                    return False

                producer.produce(
                    topic=topic,
                    key=encoded_key,
                    value=encoded_value,
                    on_delivery=self._on_delivery,
                )

            # Service callbacks on the same producer instance that produced the message.
            if callback_poll_timeout > 0:
                producer.poll(callback_poll_timeout)
            else:
                producer.poll(0)

            return True

        except BufferError:
            logger.warning("CloudOSProducer: producer buffer full; flushing briefly")
            try:
                with self._lock:
                    producer = self._producer
                if producer is not None:
                    producer.flush(5.0)
            except Exception as exc:
                logger.warning(
                    "CloudOSProducer: flush after buffer-full failed (non-fatal): %s",
                    exc,
                )
            return False

        except Exception as exc:
            logger.warning(
                "CloudOSProducer: produce failed (non-fatal) topic=%s key=%s error=%s",
                topic,
                key,
                exc,
            )
            return False

    def _ensure_topics(self) -> None:
        """
        Create missing topics if admin connectivity is available.
        Non-fatal if Kafka or AdminClient is unavailable.
        """
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
        except ImportError:
            logger.debug(
                "CloudOSProducer: confluent_kafka.admin unavailable; skipping topic init"
            )
            return

        try:
            admin = AdminClient({"bootstrap.servers": self._servers})
            existing = set(admin.list_topics(timeout=10).topics)

            to_create: List[NewTopic] = [
                NewTopic(
                    topic_name,
                    num_partitions=self._partitions,
                    replication_factor=self._replication,
                )
                for topic_name in self._topics.values()
                if topic_name not in existing
            ]

            if not to_create:
                logger.debug("CloudOSProducer: all Kafka topics already exist")
                return

            futures = admin.create_topics(to_create)
            for topic_name, future in futures.items():
                try:
                    future.result()
                    logger.info("CloudOSProducer: created Kafka topic %s", topic_name)
                except Exception as exc:
                    if "already exists" not in str(exc).lower():
                        logger.warning(
                            "CloudOSProducer: topic creation failed for %s: %s",
                            topic_name,
                            exc,
                        )

        except Exception as exc:
            logger.warning(
                "CloudOSProducer: topic initialization failed at %s (non-fatal): %s",
                self._servers,
                exc,
            )

    # ---------------------------------------------------------------------
    # Background flush / delivery callback
    # ---------------------------------------------------------------------

    def _flush_loop(self) -> None:
        """
        Lightweight backup daemon.

        publish_decision() now uses flush() directly on the producer instance
        that sent the message, so this loop should not compete with that path.
        It only services any remaining callbacks/messages opportunistically.
        """
        while True:
            time.sleep(_FLUSH_INTERVAL_SEC)

            with self._lock:
                producer = self._producer

            if producer is None:
                continue

            try:
                producer.poll(0)
            except Exception as exc:
                logger.debug("CloudOSProducer.flush_loop error: %s", exc)

    @staticmethod
    def _on_delivery(err: Any, msg: Any) -> None:
        """
        Kafka delivery callback.
        Produces more actionable logs without raising.
        """
        if err is None:
            raw_key = None
            try:
                raw_key = msg.key()
            except Exception:
                raw_key = None

            key = raw_key.decode("utf-8", errors="replace")[:8] if raw_key else "?"
            logger.info(
                "KafkaProducer: delivered key=%s topic=%s partition=%d offset=%d",
                key,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            return

        try:
            from confluent_kafka import KafkaError

            code = err.code()
            topic = msg.topic() if msg is not None else "?"

            if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                logger.error(
                    "KafkaProducer: topic '%s' does not exist on broker",
                    topic,
                )
            elif code == KafkaError._MSG_TIMED_OUT:
                logger.warning(
                    "KafkaProducer: message timed out for topic=%s",
                    topic,
                )
            elif code in (KafkaError._TRANSPORT, KafkaError.NETWORK_EXCEPTION):
                logger.warning(
                    "KafkaProducer: network error delivering to topic=%s",
                    topic,
                )
            else:
                logger.warning(
                    "KafkaProducer: delivery failed topic=%s error=%s",
                    topic,
                    err,
                )
        except Exception:
            logger.warning(
                "KafkaProducer: delivery failed topic=%s error=%s",
                msg.topic() if msg is not None else "?",
                err,
            )


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()