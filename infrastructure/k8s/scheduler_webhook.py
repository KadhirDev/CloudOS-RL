"""
CloudOS-RL Scheduler Webhook (Updated)
========================================
Kubernetes mutating admission webhook that intercepts Pod creation
and annotates pods with the RL scheduling decision.

This is the Kubernetes-native scheduling path (alternative to the
operator poll loop). The webhook fires on every Pod admission request
and adds scheduling annotations BEFORE the pod is scheduled.

Two operator modes — choose one:
  1. Operator poll loop  (Module C, operator.py)
     - Watches CloudWorkload CRs every N seconds
     - Good for batch workloads submitted as CRs
     - Does not intercept arbitrary pods

  2. Admission webhook   (this file)
     - Fires on every Pod creation
     - Good for intercepting existing workloads
     - Requires TLS cert + webhook registration
     - Lower latency (inline, not polling)

For development (Minikube) the poll loop operator is easier to run.
The webhook is provided for completeness and production use.

Webhook endpoint: POST /mutate
TLS: required by Kubernetes (use cert-manager or self-signed in Minikube)

Run:
  python infrastructure/k8s/scheduler_webhook.py --port 8443 --cert cert.pem --key key.pem
"""

import argparse
import base64
import json
import logging
import ssl
import sys
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 8443


def _load_config() -> dict:
    p = Path("config/settings.yaml")
    if not p.exists():
        return {}
    import yaml
    with open(p) as f:
        return yaml.safe_load(f) or {}


class WebhookHandler(BaseHTTPRequestHandler):
    """
    Handles POST /mutate — Kubernetes MutatingAdmissionWebhook requests.
    Adds cloudos.ai/* annotations to pods with the RL scheduling decision.
    """

    agent    = None   # set at startup
    config   = None

    def do_POST(self):
        if self.path != "/mutate":
            self.send_response(404)
            self.end_headers()
            return

        try:
            length  = int(self.headers.get("Content-Length", 0))
            body    = self.rfile.read(length)
            review  = json.loads(body)
            response = self._handle_review(review)
            self._send_json(200, response)
        except Exception as exc:
            logger.error("WebhookHandler: error: %s", exc, exc_info=True)
            self._send_json(500, {"error": str(exc)})

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        logger.debug("webhook: " + fmt, *args)

    # -----------------------------------------------------------------------

    def _handle_review(self, review: Dict) -> Dict:
        """
        Processes an AdmissionReview request.
        Returns an AdmissionReview response with patch annotations.
        """
        request = review.get("request", {})
        uid     = request.get("uid", str(uuid.uuid4()))
        obj     = request.get("object", {})
        kind    = request.get("kind",   {}).get("kind", "")

        # Only mutate Pods
        if kind != "Pod":
            return self._allow(uid, patch=None)

        # Extract workload hints from pod annotations/labels
        meta        = obj.get("metadata", {})
        annotations = meta.get("annotations", {})
        labels      = meta.get("labels",      {})

        # Build workload dict from pod spec
        workload = self._pod_to_workload(obj, meta)

        # Get RL decision
        decision = self._get_decision(workload)
        if not decision:
            return self._allow(uid, patch=None)

        # Build annotation patch
        new_annotations = {
            **annotations,
            "cloudos.ai/scheduled-cloud":    decision.get("cloud",           "aws"),
            "cloudos.ai/scheduled-region":   decision.get("region",          "us-east-1"),
            "cloudos.ai/instance-type":      decision.get("instance_type",   "m5.large"),
            "cloudos.ai/purchase-option":    decision.get("purchase_option", "on_demand"),
            "cloudos.ai/cost-per-hr":        str(round(decision.get("estimated_cost_per_hr", 0), 4)),
            "cloudos.ai/cost-savings-pct":   str(round(decision.get("cost_savings_pct", 0), 1)),
            "cloudos.ai/carbon-savings-pct": str(round(decision.get("carbon_savings_pct", 0), 1)),
            "cloudos.ai/decision-id":        decision.get("decision_id", str(uuid.uuid4())),
            "cloudos.ai/explanation":        decision.get("explanation", {}).get("summary", ""),
        }

        patch = [
            {
                "op":    "replace" if annotations else "add",
                "path":  "/metadata/annotations",
                "value": new_annotations,
            }
        ]

        return self._allow(uid, patch=patch)

    def _pod_to_workload(self, obj: Dict, meta: Dict) -> Dict:
        """Builds a minimal workload dict from pod spec for RL input."""
        spec        = obj.get("spec", {})
        containers  = spec.get("containers", [{}])
        resources   = containers[0].get("resources", {}) if containers else {}
        requests    = resources.get("requests", {})
        labels      = meta.get("labels", {})

        return {
            "workload_id":             meta.get("name", "pod"),
            "cpu_request_vcpu":        _parse_cpu_str(requests.get("cpu",    "0.5")),
            "memory_request_gb":       _parse_mem_str(requests.get("memory", "512Mi")),
            "gpu_count":               0,
            "storage_gb":              50.0,
            "network_bandwidth_gbps":  1.0,
            "expected_duration_hours": 1.0,
            "priority":                int(labels.get("cloudos.ai/priority", 2)),
            "sla_latency_ms":          200,
            "workload_type":           labels.get("cloudos.ai/workload-type", "batch"),
            "workload_type_encoded":   2,
            "is_spot_tolerant":        int(labels.get("cloudos.ai/spot-tolerant", "false").lower() == "true"),
            "constraints":             {},
        }

    def _get_decision(self, workload: Dict) -> Optional[Dict]:
        if self.agent is None:
            return None
        try:
            return self.agent.decide(workload)
        except Exception as exc:
            logger.warning("webhook: agent.decide failed: %s", exc)
            return None

    # -----------------------------------------------------------------------

    @staticmethod
    def _allow(uid: str, patch: Optional[list]) -> Dict:
        response: Dict = {
            "apiVersion": "admission.k8s.io/v1",
            "kind":       "AdmissionReview",
            "response": {
                "uid":     uid,
                "allowed": True,
            },
        }
        if patch:
            patch_b64 = base64.b64encode(json.dumps(patch).encode()).decode()
            response["response"]["patchType"] = "JSONPatch"
            response["response"]["patch"]     = patch_b64
        return response

    def _send_json(self, code: int, body: Dict):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_cpu_str(s: str) -> float:
    if s.endswith("m"):
        return float(s[:-1]) / 1000
    try:
        return float(s)
    except ValueError:
        return 0.5


def _parse_mem_str(s: str) -> float:
    import re
    m = re.match(r"([0-9.]+)([A-Za-z]*)", str(s))
    if not m:
        return 0.5
    n, u = float(m.group(1)), m.group(2).lower()
    return n / 1024 if u in ("mi", "mb") else n if u in ("gi", "gb") else n / (1024 * 1024)


# ── Server ───────────────────────────────────────────────────────────────────

def run_webhook(port: int, certfile: Optional[str], keyfile: Optional[str]):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-40s  %(levelname)s  %(message)s",
    )
    log = logging.getLogger("cloudos.webhook")

    config = _load_config()

    # Load agent
    try:
        from ai_engine.inference.scheduler_agent import SchedulerAgent
        agent = SchedulerAgent.load(config=config, with_explainer=False)
        WebhookHandler.agent  = agent
        WebhookHandler.config = config
        if agent:
            log.info("Webhook: SchedulerAgent loaded")
        else:
            log.warning("Webhook: agent not loaded — will allow pods without annotation")
    except Exception as exc:
        log.warning("Webhook: agent load failed (%s) — running in passthrough mode", exc)

    server = HTTPServer(("0.0.0.0", port), WebhookHandler)

    if certfile and keyfile and Path(certfile).exists() and Path(keyfile).exists():
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile, keyfile)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        log.info("Webhook: TLS enabled (cert=%s)", certfile)
    else:
        log.warning("Webhook: running WITHOUT TLS (development only)")

    log.info("Webhook: listening on :%d /mutate", port)
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default=_DEFAULT_PORT, type=int)
    parser.add_argument("--cert", default=None)
    parser.add_argument("--key",  default=None)
    args = parser.parse_args()
    run_webhook(args.port, args.cert, args.key)