"""
API Pydantic Schemas
=====================
All request/response models for the CloudOS-RL scheduling API.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =============================================================================
# Request schemas
# =============================================================================

class WorkloadRequest(BaseModel):
    """
    Workload submission payload.
    All fields optional except workload_type — defaults reflect a typical
    batch ML training job.
    """
    workload_id: Optional[str] = Field(default=None)
    workload_type: str = Field(
        default="batch",
        pattern="^(training|inference|batch|streaming)$",
    )
    cpu_request_vcpu: float = Field(default=2.0, ge=0.25, le=512.0)
    memory_request_gb: float = Field(default=4.0, ge=0.5, le=2048.0)
    gpu_count: int = Field(default=0, ge=0, le=16)
    storage_gb: float = Field(default=50.0, ge=1.0)
    network_bandwidth_gbps: float = Field(default=1.0, ge=0.1)
    expected_duration_hours: float = Field(default=1.0, ge=0.1)
    priority: int = Field(default=2, ge=1, le=4)
    sla_latency_ms: int = Field(default=200, ge=10)
    sla_tier: str = Field(default="standard")
    is_spot_tolerant: bool = Field(default=False)
    constraints: Dict[str, Any] = Field(default_factory=dict)

    def to_agent_dict(self) -> Dict[str, Any]:
        """Converts to the flat dict format expected by SchedulerAgent.decide()."""
        return {
            "workload_id": self.workload_id or str(uuid.uuid4()),
            "workload_type": self.workload_type,
            "workload_type_encoded": {
                "training": 0,
                "inference": 1,
                "batch": 2,
                "streaming": 3,
            }.get(self.workload_type, 2),
            "cpu_request_vcpu": self.cpu_request_vcpu,
            "memory_request_gb": self.memory_request_gb,
            "gpu_count": self.gpu_count,
            "storage_gb": self.storage_gb,
            "network_bandwidth_gbps": self.network_bandwidth_gbps,
            "expected_duration_hours": self.expected_duration_hours,
            "priority": self.priority,
            "sla_latency_ms": self.sla_latency_ms,
            "sla_tier": self.sla_tier,
            "is_spot_tolerant": int(self.is_spot_tolerant),
            "constraints": self.constraints,
        }


class BatchWorkloadRequest(BaseModel):
    workloads: List[WorkloadRequest] = Field(..., min_length=1, max_length=50)


# =============================================================================
# Response schemas
# =============================================================================

class ExplanationResponse(BaseModel):
    summary: str = ""
    top_drivers: List[Dict[str, Any]] = Field(default_factory=list)
    base_value: float = 0.0
    top_positive: List[Dict[str, Any]] = Field(default_factory=list)
    top_negative: List[Dict[str, Any]] = Field(default_factory=list)
    confidence: float = 0.0
    explanation_ms: float = 0.0


class SchedulingDecision(BaseModel):
    decision_id: str
    workload_id: str
    cloud: str
    region: str
    instance_type: str
    purchase_option: str
    sla_tier: str = "standard"
    estimated_cost_per_hr: float
    cost_savings_pct: float
    carbon_savings_pct: float
    latency_ms: float
    explanation: Optional[Dict[str, Any]] = None

    @field_validator("cost_savings_pct", "carbon_savings_pct", mode="before")
    @classmethod
    def clamp_pct(cls, v: Any) -> float:
        return max(0.0, min(100.0, float(v or 0)))


class BatchSchedulingResponse(BaseModel):
    decisions: List[SchedulingDecision]
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    count: int
    total_latency_ms: float


class DecisionListResponse(BaseModel):
    decisions: List[SchedulingDecision]
    count: int


class AgentStatusResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    agent_loaded: bool
    model_path: str
    shap_ready: bool
    background_shape: List[int]
    decisions_served: int
    last_decision_id: Optional[str] = None
    last_decision_cloud: Optional[str] = None
    last_decision_region: Optional[str] = None