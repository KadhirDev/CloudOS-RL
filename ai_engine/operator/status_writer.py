"""
Status Writer
==============
Patches CloudWorkload CR status subresource via kubectl.

After the RL agent makes a decision, the operator calls:
    StatusWriter.write(name, namespace, decision)

Which issues:
    kubectl patch cloudworkload <name> -n <namespace>
      --subresource=status
      --type=merge
      -p '{"status": {"phase": "Scheduled", "scheduledCloud": "aws", ...}}'

The status patch is what makes:
    kubectl get cloudworkloads     — show Cloud/Region/CostSavings columns
    kubectl describe cloudworkload — show full explanation

Status schema (matches crd.yaml status section):
    phase:                Pending | Scheduling | Scheduled | Running | Completed | Failed
    scheduledCloud:       aws | gcp | azure
    scheduledRegion:      us-east-1 | eu-north-1 | ...
    instanceType:         m5.large | ...
    purchaseOption:       on_demand | spot | reserved_1yr | reserved_3yr
    estimatedCostPerHr:   float
    costSavingsPct:       str  "28.4%"
    carbonSavingsPct:     str  "19.2%"
    schedulingLatencyMs:  float
    decisionId:           str
    scheduledAt:          ISO 8601
    message:              str
    explanation:          dict (from Module A SHAP formatter)
"""

import json
import logging
import subprocess
import time
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class StatusWriter:
    """
    Patches CloudWorkload status via kubectl.
    Uses --subresource=status to avoid needing full resource write permission.

    Supports dry_run mode — logs patches without actually applying them.
    """

    def __init__(self, dry_run: bool = False):
        self._dry_run = dry_run
        if dry_run:
            logger.info("StatusWriter: DRY RUN mode — patches will be logged but not applied")

    # -----------------------------------------------------------------------
    # Public
    # -----------------------------------------------------------------------

    def set_scheduling(self, name: str, namespace: str) -> bool:
        """Marks workload as phase=Scheduling (in-progress)."""
        return self._patch(name, namespace, {
            "phase":   "Scheduling",
            "message": "RL agent is computing placement decision.",
        })

    def set_scheduled(
        self,
        name:      str,
        namespace: str,
        decision:  Dict,
    ) -> bool:
        """
        Writes the full RL scheduling decision to the CR status.

        Args:
            name:      CloudWorkload name
            namespace: Kubernetes namespace
            decision:  dict from SchedulerAgent.decide()

        Returns:
            True if patch succeeded.
        """
        explanation  = decision.get("explanation", {})
        summary      = explanation.get("summary", "") if isinstance(explanation, dict) else ""
        cost_savings = decision.get("cost_savings_pct", 0.0)
        carb_savings = decision.get("carbon_savings_pct", 0.0)

        status = {
            "phase":               "Scheduled",
            "scheduledCloud":      decision.get("cloud",           "aws"),
            "scheduledRegion":     decision.get("region",          "us-east-1"),
            "instanceType":        decision.get("instance_type",   "m5.large"),
            "purchaseOption":      decision.get("purchase_option", "on_demand"),
            "estimatedCostPerHr":  round(float(decision.get("estimated_cost_per_hr", 0.0)), 4),
            "costSavingsPct":      f"{cost_savings:.1f}%",
            "carbonSavingsPct":    f"{carb_savings:.1f}%",
            "schedulingLatencyMs": round(float(decision.get("latency_ms", 0.0)), 2),
            "decisionId":          decision.get("decision_id", ""),
            "scheduledAt":         _now_iso(),
            "message":             summary or f"Scheduled to {decision.get('cloud')}/{decision.get('region')}",
            "explanation":         self._safe_explanation(explanation),
        }
        return self._patch(name, namespace, status)

    def set_failed(
        self,
        name:      str,
        namespace: str,
        reason:    str,
    ) -> bool:
        """Marks workload as phase=Failed with error message."""
        return self._patch(name, namespace, {
            "phase":       "Failed",
            "message":     reason[:500],
            "scheduledAt": _now_iso(),
        })

    def set_phase(
        self,
        name:      str,
        namespace: str,
        phase:     str,
        message:   str = "",
    ) -> bool:
        """Generic phase update."""
        return self._patch(name, namespace, {
            "phase":   phase,
            "message": message,
        })

    # -----------------------------------------------------------------------
    # Private
    # -----------------------------------------------------------------------

    def _patch(self, name: str, namespace: str, status_fields: Dict) -> bool:
        """
        Issues:
          kubectl patch cloudworkload <name> -n <namespace>
            --subresource=status --type=merge -p <json>
        """
        patch_body = json.dumps({"status": status_fields})

        if self._dry_run:
            logger.info(
                "[DRY RUN] patch cloudworkload/%s -n %s status: %s",
                name, namespace, json.dumps(status_fields, indent=2)
            )
            return True

        cmd = [
            "kubectl", "patch", "cloudworkload", name,
            "-n",             namespace,
            "--subresource=status",
            "--type=merge",
            "-p",             patch_body,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                logger.debug(
                    "StatusWriter: patched %s/%s → phase=%s",
                    namespace, name, status_fields.get("phase", "?")
                )
                return True
            else:
                logger.error(
                    "StatusWriter: patch failed for %s/%s: %s",
                    namespace, name, result.stderr.strip()[:300]
                )
                return False
        except subprocess.TimeoutExpired:
            logger.error("StatusWriter: patch timed out for %s/%s", namespace, name)
            return False
        except FileNotFoundError:
            logger.error("StatusWriter: kubectl not found in PATH")
            return False
        except Exception as exc:
            logger.error("StatusWriter: unexpected error for %s/%s: %s", namespace, name, exc)
            return False

    @staticmethod
    def _safe_explanation(explanation) -> Dict:
        """
        Returns a serialisable explanation dict.
        Strips large shap_values dict to keep the CR status small.
        Only the top_drivers summary is stored in status.
        """
        if not isinstance(explanation, dict):
            return {}
        return {
            "summary":        explanation.get("summary",        ""),
            "top_drivers":    explanation.get("top_drivers",    [])[:3],
            "confidence":     explanation.get("confidence",     0.0),
            "explanation_ms": explanation.get("explanation_ms", 0.0),
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")