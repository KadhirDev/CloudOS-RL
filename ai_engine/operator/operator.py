"""
CloudOS-RL Kubernetes Operator
================================
Controller loop that watches CloudWorkload custom resources
and drives the RL scheduling decision pipeline.

Architecture:
  ┌─────────────────────────────────────────────────────┐
  │  CloudOSOperator (main loop)                        │
  │                                                     │
  │  poll_loop()  ←──── runs every poll_interval_sec   │
  │      │                                              │
  │      ├── list_pending()  ←── kubectl get cw         │
  │      │       phase in (Pending, "")                 │
  │      │                                              │
  │      └── for each pending workload:                 │
  │              set_phase(Scheduling)                  │
  │              workload = WorkloadMapper.map(cr)      │
  │              decision = SchedulerAgent.decide()     │
  │              decision_id = uuid4()                  │
  │              kafka_producer.publish_decision()      │
  │              set_scheduled(decision)                │
  └─────────────────────────────────────────────────────┘

Operator modes:
  --dry-run      logs decisions but does not patch CR status or send to Kafka
  --no-kafka     makes decisions + patches status, but skips Kafka publishing
  --no-shap      skips SHAP explainability (faster startup, no explanation field)

Compatible with:
  Module A — SchedulerAgent.decide() + SHAP explanation
  Module D — KafkaProducer.publish_decision()
  Module F — CloudWorkload CRD schema
"""

import json
import logging
import subprocess
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from ai_engine.operator.workload_mapper import WorkloadMapper
from ai_engine.operator.status_writer   import StatusWriter

logger = logging.getLogger(__name__)

_DEFAULT_POLL_SEC    = 5
_DEFAULT_NAMESPACE   = "cloudos-rl"
_DECISION_TIMEOUT_MS = 500   # warn if decision takes longer


class CloudOSOperator:
    """
    Kubernetes operator controller loop for CloudOS-RL.

    Watches CloudWorkload CRs in the configured namespace.
    For each workload in phase=Pending, calls the RL agent
    and patches the CR status with the scheduling decision.
    """

    def __init__(
        self,
        config:       Dict,
        dry_run:      bool = False,
        no_kafka:     bool = False,
        no_shap:      bool = False,
        namespace:    str  = _DEFAULT_NAMESPACE,
        poll_interval:int  = _DEFAULT_POLL_SEC,
    ):
        self._config        = config
        self._dry_run       = dry_run
        self._no_kafka      = no_kafka
        self._namespace     = namespace
        self._poll_interval = poll_interval

        self._mapper  = WorkloadMapper()
        self._writer  = StatusWriter(dry_run=dry_run)
        self._agent   = None        # loaded lazily
        self._producer= None        # loaded lazily

        self._no_shap    = no_shap
        self._stats      = {"processed": 0, "errors": 0, "skipped": 0}
        self._seen_rv    : Dict[str, str] = {}  # name → resourceVersion (dedup)

        logger.info(
            "CloudOSOperator: namespace=%s poll=%ds dry_run=%s no_kafka=%s no_shap=%s",
            namespace, poll_interval, dry_run, no_kafka, no_shap,
        )

    # -----------------------------------------------------------------------
    # Public — lifecycle
    # -----------------------------------------------------------------------

    def start(self):
        """
        Starts the operator controller loop.
        Blocks indefinitely. Use Ctrl+C to stop.
        """
        logger.info("CloudOSOperator: loading agent ...")
        self._agent = self._load_agent()

        if not self._no_kafka:
            logger.info("CloudOSOperator: connecting Kafka producer ...")
            self._producer = self._load_producer()

        logger.info("CloudOSOperator: entering poll loop (every %ds) ...", self._poll_interval)
        logger.info("CloudOSOperator: watching namespace '%s' for CloudWorkloads ...", self._namespace)

        try:
            while True:
                try:
                    self._poll_once()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    logger.error("CloudOSOperator: poll error: %s", exc, exc_info=True)
                time.sleep(self._poll_interval)
        except KeyboardInterrupt:
            logger.info("CloudOSOperator: received shutdown signal.")
        finally:
            self._shutdown()

    def run_once(self) -> int:
        """
        Runs a single poll cycle.
        Useful for testing without starting the full loop.
        Returns the number of workloads processed.
        """
        if self._agent is None:
            self._agent = self._load_agent()
        if self._producer is None and not self._no_kafka:
            self._producer = self._load_producer()
        return self._poll_once()

    # -----------------------------------------------------------------------
    # Private — poll loop
    # -----------------------------------------------------------------------

    def _poll_once(self) -> int:
        """
        Lists all Pending CloudWorkloads and processes each one.
        Returns the count of workloads processed this cycle.
        """
        pending = self._list_pending()
        if not pending:
            return 0

        logger.info("CloudOSOperator: found %d pending workload(s)", len(pending))
        count = 0

        for cr in pending:
            name = cr.get("metadata", {}).get("name", "unknown")
            rv   = cr.get("metadata", {}).get("resourceVersion", "")

            # Skip if we already processed this exact version
            if self._seen_rv.get(name) == rv:
                self._stats["skipped"] += 1
                continue

            success = self._process(cr)
            if success:
                self._seen_rv[name] = rv
                self._stats["processed"] += 1
                count += 1
            else:
                self._stats["errors"] += 1

        return count

    def _process(self, cr: Dict) -> bool:
        """
        Processes a single CloudWorkload CR through the full pipeline:
          1. Patch phase=Scheduling
          2. Map CR spec → workload dict
          3. RL agent decide()
          4. Kafka publish
          5. Patch phase=Scheduled with decision

        Returns True on success.
        """
        meta      = cr.get("metadata", {})
        name      = meta.get("name",      "unknown")
        namespace = meta.get("namespace", self._namespace)

        logger.info("CloudOSOperator: processing workload '%s/%s'", namespace, name)

        # Step 1 — mark as Scheduling
        self._writer.set_scheduling(name, namespace)

        # Step 2 — map CR spec to workload dict
        workload = self._mapper.map(cr)
        if workload is None:
            reason = f"WorkloadMapper failed to parse spec for '{name}'"
            logger.error("CloudOSOperator: %s", reason)
            self._writer.set_failed(name, namespace, reason)
            return False

        # Step 3 — RL agent decision
        t0 = time.perf_counter()
        try:
            decision = self._make_decision(workload)
        except Exception as exc:
            reason = f"RL agent error: {exc}"
            logger.error("CloudOSOperator: %s", reason, exc_info=True)
            self._writer.set_failed(name, namespace, reason)
            return False

        latency_ms = (time.perf_counter() - t0) * 1000
        if latency_ms > _DECISION_TIMEOUT_MS:
            logger.warning(
                "CloudOSOperator: decision for '%s' took %.0fms (target <500ms)",
                name, latency_ms,
            )

        decision_id = str(uuid.uuid4())
        decision["decision_id"] = decision_id
        decision["workload_id"] = name

        logger.info(
            "CloudOSOperator: decision for '%s' → %s/%s %s (cost=%.4f/hr, savings=%.1f%%, %.0fms)",
            name,
            decision.get("cloud"),
            decision.get("region"),
            decision.get("purchase_option"),
            decision.get("estimated_cost_per_hr", 0),
            decision.get("cost_savings_pct",      0),
            latency_ms,
        )

        # Step 4 — Kafka publish
        if not self._no_kafka and self._producer is not None:
            self._publish(decision, workload)

        # Step 5 — Patch CR status
        ok = self._writer.set_scheduled(name, namespace, decision)
        if not ok:
            logger.error(
                "CloudOSOperator: status patch failed for '%s' (decision was still made)",
                name,
            )
            return False

        return True

    # -----------------------------------------------------------------------
    # Private — decision
    # -----------------------------------------------------------------------

    def _make_decision(self, workload: Dict) -> Dict:
        """
        Calls SchedulerAgent.decide() if model is loaded.
        Falls back to a deterministic heuristic if model not available.
        """
        if self._agent is not None:
            decision = self._agent.decide(workload)
            if decision:
                return decision
            logger.warning("CloudOSOperator: agent returned None — using heuristic fallback")

        return self._heuristic_decision(workload)

    def _heuristic_decision(self, workload: Dict) -> Dict:
        """
        Deterministic fallback used when the PPO model is not yet trained.
        Picks eu-north-1 (cleanest carbon, 42 gCO2/kWh) on spot if spot-tolerant,
        otherwise us-east-1 on-demand.
        """
        spot_ok  = bool(workload.get("is_spot_tolerant", 0))
        cloud    = "aws"
        region   = "eu-north-1" if spot_ok else "us-east-1"
        purchase = "spot"       if spot_ok else "on_demand"

        return {
            "cloud":               cloud,
            "region":              region,
            "instance_type":       "m5.large",
            "purchase_option":     purchase,
            "sla_tier":            workload.get("sla_tier", "standard"),
            "estimated_cost_per_hr": 0.032 if spot_ok else 0.096,
            "cost_savings_pct":    66.7   if spot_ok else 0.0,
            "carbon_savings_pct":  89.9   if spot_ok else 0.0,
            "latency_ms":          1.5,
            "explanation": {
                "summary":     f"Heuristic decision: {cloud}/{region} ({purchase}). "
                               f"RL model not yet trained.",
                "top_drivers": [],
                "confidence":  0.0,
            },
        }

    # -----------------------------------------------------------------------
    # Private — Kafka
    # -----------------------------------------------------------------------

    def _publish(self, decision: Dict, workload: Dict):
        """Publishes decision to Kafka. Swallows errors to avoid blocking the operator."""
        try:
            self._producer.publish_decision({
                **decision,
                "workload_type": workload.get("workload_type", "batch"),
            })
            logger.debug("CloudOSOperator: published decision %s to Kafka", decision.get("decision_id"))
        except Exception as exc:
            logger.warning("CloudOSOperator: Kafka publish failed (non-fatal): %s", exc)

    # -----------------------------------------------------------------------
    # Private — list pending CRs
    # -----------------------------------------------------------------------

    def _list_pending(self) -> List[Dict]:
        """
        Lists CloudWorkload CRs in phase=Pending (or phase unset).
        Uses kubectl get cloudworkloads -o json.
        """
        cmd = [
            "kubectl", "get", "cloudworkloads",
            "-n",     self._namespace,
            "-o",     "json",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                logger.warning(
                    "CloudOSOperator: kubectl get failed: %s",
                    result.stderr.strip()[:200]
                )
                return []

            data  = json.loads(result.stdout)
            items = data.get("items", [])

            pending = []
            for item in items:
                phase = item.get("status", {}).get("phase", "")
                if phase in ("", "Pending"):
                    pending.append(item)

            return pending

        except subprocess.TimeoutExpired:
            logger.warning("CloudOSOperator: kubectl timed out")
            return []
        except json.JSONDecodeError as exc:
            logger.warning("CloudOSOperator: JSON parse error: %s", exc)
            return []
        except Exception as exc:
            logger.error("CloudOSOperator: list_pending error: %s", exc)
            return []

    # -----------------------------------------------------------------------
    # Private — loader helpers
    # -----------------------------------------------------------------------

    def _load_agent(self):
        """
        Loads SchedulerAgent. Returns None if model not found.
        Operator continues in heuristic mode if model not yet trained.
        """
        try:
            from ai_engine.inference.scheduler_agent import SchedulerAgent
            agent = SchedulerAgent.load(
                config=self._config,
                with_explainer=(not self._no_shap),
            )
            if agent:
                logger.info("CloudOSOperator: SchedulerAgent loaded (PPO + SHAP mode)")
            else:
                logger.warning(
                    "CloudOSOperator: SchedulerAgent.load returned None "
                    "(model not trained yet) — using heuristic fallback"
                )
            return agent
        except Exception as exc:
            logger.warning(
                "CloudOSOperator: agent load failed (%s) — using heuristic fallback", exc
            )
            return None

    def _load_producer(self):
        """Loads KafkaProducer. Returns None if Kafka not reachable."""
        try:
            from ai_engine.kafka.producer import CloudOSProducer
            p = CloudOSProducer(self._config)
            logger.info("CloudOSOperator: Kafka producer ready")
            return p
        except Exception as exc:
            logger.warning(
                "CloudOSOperator: Kafka producer load failed (%s) — decisions will not be published",
                exc,
            )
            return None

    def _shutdown(self):
        """Clean shutdown."""
        if self._producer is not None:
            try:
                self._producer.flush()
            except Exception:
                pass
        logger.info(
            "CloudOSOperator: shutdown — processed=%d errors=%d skipped=%d",
            self._stats["processed"],
            self._stats["errors"],
            self._stats["skipped"],
        )