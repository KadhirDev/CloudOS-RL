"""
Scheduling API Routes
======================
POST /api/v1/schedule       — submit workload, get RL placement decision
GET  /api/v1/decisions      — list recent decisions (last 100)
GET  /api/v1/decisions/{id} — get single decision with full SHAP explanation
POST /api/v1/batch          — submit multiple workloads, get decisions in parallel
GET  /api/v1/status         — agent status (model loaded, SHAP ready, last decision)
"""

import logging
import time
import uuid
from typing import Any, Dict, Optional

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from ai_engine.operator.operator import CloudOSOperator
from backend.api.models.schemas import (
    AgentStatusResponse,
    BatchSchedulingResponse,
    BatchWorkloadRequest,
    DecisionListResponse,
    SchedulingDecision,
    WorkloadRequest,
)
from backend.core.agent_singleton import get_agent, get_producer
from backend.core.decision_store import DecisionStore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["scheduling"])

# Module-level decision store (in-memory ring buffer, last 1000 decisions)
_decision_store = DecisionStore(max_size=1000)

# Integer -> label mapping for model/action output
SLA_TIER_MAP = {
    0: "best_effort",
    1: "bronze",
    2: "silver",
    3: "gold",
    4: "platinum",
    5: "critical",
}


# =============================================================================
# Internal helpers
# =============================================================================

def _load_config() -> dict:
    """Load application config safely."""
    try:
        with open("config/settings.yaml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.warning("config/settings.yaml not found; using empty config")
        return {}
    except Exception as exc:
        logger.warning("Failed to load config/settings.yaml: %s", exc)
        return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float safely."""
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_str(value: Any, default: str = "") -> str:
    """Convert value to string safely."""
    if value is None:
        return default
    return str(value)


def _normalize_sla_tier(value: Any) -> str:
    """Normalize SLA tier from int/model output into API string."""
    if value is None:
        return "standard"
    if isinstance(value, int):
        return SLA_TIER_MAP.get(value, "standard")
    if isinstance(value, float) and value.is_integer():
        return SLA_TIER_MAP.get(int(value), "standard")
    return str(value)


def _to_scheduling_decision(
    raw: Dict[str, Any],
    *,
    workload_id: str,
    decision_id: str,
    latency_ms: float,
) -> SchedulingDecision:
    """
    Convert raw scheduler/operator output into the API response model safely.

    This prevents pydantic validation crashes when the RL agent returns:
    - sla_tier as int instead of string
    - missing estimated_cost_per_hr / savings fields
    - missing explanation
    """
    raw = raw or {}

    return SchedulingDecision(
        decision_id=_safe_str(raw.get("decision_id", decision_id), decision_id),
        workload_id=_safe_str(raw.get("workload_id", workload_id), workload_id),
        cloud=_safe_str(raw.get("cloud", "unknown"), "unknown"),
        region=_safe_str(raw.get("region", "unknown"), "unknown"),
        instance_type=_safe_str(raw.get("instance_type", "unknown"), "unknown"),
        scaling_action=_safe_str(raw.get("scaling_action", "none"), "none"),
        purchase_option=_safe_str(raw.get("purchase_option", "on_demand"), "on_demand"),
        sla_tier=_normalize_sla_tier(raw.get("sla_tier", "standard")),
        estimated_cost_per_hr=_safe_float(raw.get("estimated_cost_per_hr", 0.0), 0.0),
        cost_savings_pct=_safe_float(raw.get("cost_savings_pct", 0.0), 0.0),
        carbon_savings_pct=_safe_float(raw.get("carbon_savings_pct", 0.0), 0.0),
        latency_ms=_safe_float(raw.get("latency_ms", latency_ms), latency_ms),
        explanation=raw.get("explanation") or {},
        actual_reward=raw.get("actual_reward"),
    )


def _heuristic_fallback_decision(request: WorkloadRequest) -> SchedulingDecision:
    """
    Use CloudOSOperator heuristic decision path when the RL agent
    is still loading, instead of returning HTTP 503.
    """
    config = _load_config()

    op = CloudOSOperator(
        config=config,
        dry_run=True,
        no_kafka=True,
        no_shap=True,
    )
    op._agent = None

    workload = request.to_agent_dict()

    fallback_workload_id = (
        getattr(request, "workload_id", None)
        or workload.get("workload_id")
        or "api-fallback"
    )
    fallback_decision_id = str(uuid.uuid4())

    workload["workload_id"] = fallback_workload_id

    raw = op._heuristic_decision(workload) or {}
    raw["decision_id"] = fallback_decision_id
    raw["workload_id"] = fallback_workload_id
    raw["latency_ms"] = 1.0

    return _to_scheduling_decision(
        raw,
        workload_id=fallback_workload_id,
        decision_id=fallback_decision_id,
        latency_ms=1.0,
    )


# =============================================================================
# POST /api/v1/schedule
# =============================================================================

@router.post(
    "/schedule",
    response_model=SchedulingDecision,
    status_code=status.HTTP_200_OK,
    summary="Submit a workload for RL scheduling",
    description="""
    Accepts a workload specification and returns an RL-generated placement
    decision including cloud, region, instance type, purchase option,
    cost/carbon savings, and SHAP explainability.

    Decision latency target: p95 < 100ms.
    """,
)
async def schedule_workload(
    request: WorkloadRequest,
    background_tasks: BackgroundTasks,
    agent=Depends(get_agent),
    producer=Depends(get_producer),
) -> SchedulingDecision:
    if agent is None:
        decision = _heuristic_fallback_decision(request)
        background_tasks.add_task(
            _publish_and_store,
            producer,
            decision,
            request.to_agent_dict(),
            _decision_store,
        )
        return decision

    t0 = time.perf_counter()
    decision_id = str(uuid.uuid4())

    workload = request.to_agent_dict()
    workload_id = getattr(request, "workload_id", None) or decision_id
    workload["workload_id"] = workload_id

    try:
        # Keep compatibility with either decide(...) or schedule(...)
        if hasattr(agent, "decide"):
            raw = agent.decide(workload)
        else:
            raw = agent.schedule(workload)
    except Exception as exc:
        logger.error("SchedulerAgent inference failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"RL inference error: {str(exc)[:200]}",
        )

    if raw is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Agent returned no decision. Model may be uninitialised.",
        )

    total_latency_ms = round((time.perf_counter() - t0) * 1000, 2)

    raw = dict(raw)
    raw["decision_id"] = decision_id
    raw["workload_id"] = workload_id
    raw["latency_ms"] = total_latency_ms

    decision = _to_scheduling_decision(
        raw,
        workload_id=workload_id,
        decision_id=decision_id,
        latency_ms=total_latency_ms,
    )

    background_tasks.add_task(
        _publish_and_store,
        producer,
        decision,
        workload,
        _decision_store,
    )

    logger.info(
        "Decision %s: %s/%s %s cost=%.4f/hr savings=%.1f%% %.0fms",
        decision.decision_id[:8],
        decision.cloud,
        decision.region,
        decision.purchase_option,
        decision.estimated_cost_per_hr,
        decision.cost_savings_pct,
        decision.latency_ms,
    )

    return decision


# =============================================================================
# POST /api/v1/batch
# =============================================================================

@router.post(
    "/batch",
    response_model=BatchSchedulingResponse,
    status_code=status.HTTP_200_OK,
    summary="Submit multiple workloads for batch scheduling",
)
async def schedule_batch(
    request: BatchWorkloadRequest,
    background_tasks: BackgroundTasks,
    agent=Depends(get_agent),
    producer=Depends(get_producer),
) -> BatchSchedulingResponse:
    if len(request.workloads) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 workloads per batch")

    decisions = []
    errors = []
    t0 = time.perf_counter()

    for i, wl_req in enumerate(request.workloads):
        try:
            if agent is None:
                d = _heuristic_fallback_decision(wl_req)
                decisions.append(d)
                background_tasks.add_task(
                    _publish_and_store,
                    producer,
                    d,
                    wl_req.to_agent_dict(),
                    _decision_store,
                )
                continue

            item_t0 = time.perf_counter()
            workload = wl_req.to_agent_dict()
            decision_id = str(uuid.uuid4())
            workload_id = getattr(wl_req, "workload_id", None) or f"batch-{i}"
            workload["workload_id"] = workload_id

            if hasattr(agent, "decide"):
                raw = agent.decide(workload)
            else:
                raw = agent.schedule(workload)

            if raw:
                item_latency_ms = round((time.perf_counter() - item_t0) * 1000, 2)
                raw = dict(raw)
                raw["decision_id"] = decision_id
                raw["workload_id"] = workload_id
                raw["latency_ms"] = item_latency_ms

                d = _to_scheduling_decision(
                    raw,
                    workload_id=workload_id,
                    decision_id=decision_id,
                    latency_ms=item_latency_ms,
                )
                decisions.append(d)

                background_tasks.add_task(
                    _publish_and_store,
                    producer,
                    d,
                    workload,
                    _decision_store,
                )
            else:
                errors.append(
                    {
                        "index": i,
                        "error": "Agent returned no decision. Model may be uninitialised.",
                    }
                )
        except Exception as exc:
            logger.error("Batch scheduling failed for index %s: %s", i, exc, exc_info=True)
            errors.append({"index": i, "error": str(exc)[:200]})

    return BatchSchedulingResponse(
        decisions=decisions,
        errors=errors,
        total_latency_ms=round((time.perf_counter() - t0) * 1000, 2),
        count=len(decisions),
    )


# =============================================================================
# GET /api/v1/decisions
# =============================================================================

@router.get(
    "/decisions",
    response_model=DecisionListResponse,
    summary="List recent scheduling decisions",
)
async def list_decisions(
    limit: int = 20,
    cloud: Optional[str] = None,
    region: Optional[str] = None,
) -> DecisionListResponse:
    decisions = _decision_store.list(limit=limit, cloud=cloud, region=region)
    return DecisionListResponse(decisions=decisions, count=len(decisions))


# =============================================================================
# GET /api/v1/decisions/{decision_id}
# =============================================================================

@router.get(
    "/decisions/{decision_id}",
    response_model=SchedulingDecision,
    summary="Get a single decision with full SHAP explanation",
)
async def get_decision(decision_id: str) -> SchedulingDecision:
    d = _decision_store.get(decision_id)
    if d is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Decision {decision_id} not found. "
                f"Decisions are kept in memory for the lifetime of this pod."
            ),
        )
    return d


# =============================================================================
# GET /api/v1/status
# =============================================================================

@router.get(
    "/status",
    response_model=AgentStatusResponse,
    summary="Agent and system status",
)
async def agent_status(agent=Depends(get_agent)) -> AgentStatusResponse:
    last = _decision_store.last()

    explainer = getattr(agent, "_explainer", None) if agent else None
    config = getattr(agent, "_config", {}) if agent else {}
    model_cfg = config.get("model", {}) if isinstance(config, dict) else {}

    return AgentStatusResponse(
        agent_loaded=agent is not None,
        model_path=str(model_cfg.get("path", "")) if agent else "",
        shap_ready=explainer is not None,
        background_shape=(
            list(explainer.get_background_shape())
            if explainer is not None
            else []
        ),
        decisions_served=_decision_store.total_count(),
        last_decision_id=last.decision_id if last else None,
        last_decision_cloud=last.cloud if last else None,
        last_decision_region=last.region if last else None,
    )


# =============================================================================
# Background task helpers
# =============================================================================

async def _publish_and_store(producer, decision, workload, store):
    """
    Non-blocking: store first (always), then attempt Kafka publish.

    Store failures and Kafka failures are both non-fatal.
    Producer is allowed to be None.
    """
    try:
        store.put(decision)
    except Exception as exc:
        logger.warning("DecisionStore.put failed (non-fatal): %s", exc)

    if producer is None:
        return

    try:
        producer.publish_decision(
            {
                "decision_id": decision.decision_id,
                "workload_id": decision.workload_id,
                "cloud": decision.cloud,
                "region": decision.region,
                "instance_type": decision.instance_type,
                "purchase_option": decision.purchase_option,
                "cost_savings_pct": decision.cost_savings_pct,
                "carbon_savings_pct": decision.carbon_savings_pct,
                "latency_ms": decision.latency_ms,
                "estimated_cost_per_hr": decision.estimated_cost_per_hr,
                "workload_type": (
                    workload.get("workload_type", "batch")
                    if isinstance(workload, dict)
                    else "batch"
                ),
                "explanation": decision.explanation or {},
                "actual_reward": getattr(decision, "actual_reward", None),
            }
        )
    except Exception as exc:
        logger.warning("_publish_and_store: Kafka publish error (non-fatal): %s", exc)