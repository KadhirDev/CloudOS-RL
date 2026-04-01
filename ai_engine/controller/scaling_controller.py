"""
CloudOS-RL Scaling Controller
===============================
Optional lightweight controller that reads scheduling decisions and
optionally applies replica scaling to Kubernetes deployments.

SAFE BY DEFAULT:
  - LOG_ONLY mode: decisions are logged but never applied (default)
  - APPLY mode:    only enabled when CLOUDOS_CONTROLLER_APPLY=true

This module is COMPLETELY INDEPENDENT of the existing API, Kafka, SHAP,
and scheduling logic. It runs as a separate process.

Usage:
  python -m ai_engine.controller.scaling_controller          # log-only mode
  CLOUDOS_CONTROLLER_APPLY=true python -m ai_engine.controller.scaling_controller
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

# Feature flag — NEVER applies changes unless explicitly set
APPLY_MODE = os.environ.get("CLOUDOS_CONTROLLER_APPLY", "false").lower() == "true"
NAMESPACE   = os.environ.get("CLOUDOS_NAMESPACE", "cloudos-rl")
POLL_SEC    = int(os.environ.get("CLOUDOS_CONTROLLER_POLL_SEC", "30"))


class ScalingController:
    """
    Reads recent scheduling decisions and determines if scaling is needed.
    In APPLY mode, patches Kubernetes deployment replicas.
    In LOG_ONLY mode (default), only logs intended actions.
    """

    def __init__(self):
        mode = "APPLY" if APPLY_MODE else "LOG_ONLY"
        logger.info("ScalingController: starting in %s mode (namespace=%s)", mode, NAMESPACE)
        if APPLY_MODE:
            logger.warning(
                "ScalingController: APPLY mode active — will patch Kubernetes deployments. "
                "Set CLOUDOS_CONTROLLER_APPLY=false to disable."
            )

    def run(self):
        """Main poll loop."""
        while True:
            try:
                self._tick()
            except KeyboardInterrupt:
                logger.info("ScalingController: shutting down")
                break
            except Exception as exc:
                logger.error("ScalingController: tick error: %s", exc, exc_info=True)
            time.sleep(POLL_SEC)

    def _tick(self):
        """Single evaluation cycle."""
        decisions = self._fetch_recent_decisions()
        if not decisions:
            logger.debug("ScalingController: no recent decisions")
            return

        n = len(decisions)
        avg_latency = sum(d.get("latency_ms", 0) for d in decisions) / n

        # Simple scaling rule: if avg latency > 200ms → suggest scale up
        if avg_latency > 200.0:
            self._scale_action(
                deployment="cloudos-api",
                current_replicas=self._get_replicas("cloudos-api"),
                desired_replicas_delta=+1,
                reason=f"avg_latency={avg_latency:.0f}ms > 200ms threshold",
            )
        else:
            logger.info(
                "ScalingController: system healthy — %d decisions, avg_latency=%.0fms",
                n, avg_latency,
            )

    def _scale_action(
        self,
        deployment: str,
        current_replicas: int,
        desired_replicas_delta: int,
        reason: str,
    ):
        new_replicas = max(1, min(5, current_replicas + desired_replicas_delta))
        if new_replicas == current_replicas:
            logger.info(
                "ScalingController: %s already at replicas=%d, no change needed (%s)",
                deployment, current_replicas, reason,
            )
            return

        action = "↑ scale up" if desired_replicas_delta > 0 else "↓ scale down"
        logger.info(
            "ScalingController: %s %s: %d → %d replicas (%s)",
            action, deployment, current_replicas, new_replicas, reason,
        )

        if not APPLY_MODE:
            logger.info(
                "ScalingController: [LOG_ONLY] would patch %s to %d replicas. "
                "Set CLOUDOS_CONTROLLER_APPLY=true to enable.",
                deployment, new_replicas,
            )
            return

        self._kubectl_scale(deployment, new_replicas)

    def _kubectl_scale(self, deployment: str, replicas: int):
        cmd = [
            "kubectl", "scale", "deployment", deployment,
            f"--replicas={replicas}",
            "-n", NAMESPACE,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                logger.info("ScalingController: scaled %s to %d replicas", deployment, replicas)
            else:
                logger.error(
                    "ScalingController: kubectl scale failed: %s", result.stderr.strip()
                )
        except Exception as exc:
            logger.error("ScalingController: kubectl error: %s", exc)

    def _get_replicas(self, deployment: str) -> int:
        try:
            cmd = [
                "kubectl", "get", "deployment", deployment,
                "-n", NAMESPACE,
                "-o", "jsonpath={.spec.replicas}",
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return int(r.stdout.strip() or "1")
        except Exception:
            return 1

    def _fetch_recent_decisions(self) -> list:
        """
        Fetches recent decisions from the API.
        Falls back to empty list if API is unavailable.
        """
        try:
            import urllib.request
            url = "http://localhost:8001/api/v1/decisions?limit=20"
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read())
                return data.get("decisions", [])
        except Exception as exc:
            logger.debug("ScalingController: could not fetch decisions: %s", exc)
            return []


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-40s  %(levelname)s  %(message)s",
    )
    ScalingController().run()


if __name__ == "__main__":
    main()