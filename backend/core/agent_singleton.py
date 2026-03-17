"""
Agent Singleton
================
Loads SchedulerAgent and KafkaProducer once at API startup.

FastAPI dependency injection provides them to route handlers.
Loading is done in a background thread during startup so the
/health endpoint responds immediately while the model loads.
"""

import logging
import os
import threading
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

_agent = None
_producer = None
_lock = threading.Lock()
_ready = False


def _load_config() -> dict:
    """
    Load config from config/settings.yaml, then override selected values
    using environment variables injected by Kubernetes ConfigMap.
    """
    p = Path("config/settings.yaml")
    cfg = {}

    if p.exists():
        with open(p, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
    else:
        logger.warning("config/settings.yaml not found — using env/default config only")

    model_path = os.environ.get("CLOUDOS_MODEL_PATH", "")
    vecnorm_path = os.environ.get("CLOUDOS_VECNORM_PATH", "")
    kafka_boot = os.environ.get("CLOUDOS_KAFKA_BOOTSTRAP", "")

    if model_path:
        cfg.setdefault("model", {})
        cfg["model"]["path"] = model_path

    if vecnorm_path:
        cfg.setdefault("model", {})
        cfg["model"]["vecnorm"] = vecnorm_path

    if kafka_boot:
        cfg.setdefault("kafka", {})
        cfg["kafka"]["bootstrap_servers"] = kafka_boot

    logger.info(
        "Config resolved: model_path=%s vecnorm_path=%s kafka_bootstrap=%s",
        cfg.get("model", {}).get("path", "NOT SET"),
        cfg.get("model", {}).get("vecnorm", "NOT SET"),
        cfg.get("kafka", {}).get("bootstrap_servers", "NOT SET"),
    )

    return cfg


def _initialise() -> None:
    """
    Load the SchedulerAgent and Kafka producer once during API startup.
    """
    global _agent, _producer, _ready

    config = _load_config()

    model_path = config.get("model", {}).get("path", "")
    vecnorm_path = config.get("model", {}).get("vecnorm", "")

    mp = Path(model_path) if model_path else Path("")
    if model_path and mp.suffix == "":
        mp = Path(f"{model_path}.zip")

    logger.info(
        "AgentSingleton: resolved model file = %s exists=%s",
        str(mp) if model_path else "NOT SET",
        mp.exists() if model_path else False,
    )
    logger.info(
        "AgentSingleton: vecnorm file = %s exists=%s",
        vecnorm_path if vecnorm_path else "NOT SET",
        Path(vecnorm_path).exists() if vecnorm_path else False,
    )

    try:
        from ai_engine.inference.scheduler_agent import SchedulerAgent

        agent = SchedulerAgent.load(
            config=config,
            model_path=str(mp) if model_path else None,
            vecnorm_path=vecnorm_path or None,
            with_explainer=False,
        )

        with _lock:
            _agent = agent

        if agent:
            shap_ready = getattr(agent, "_explainer", None) is not None
            logger.info(
                "AgentSingleton: SchedulerAgent ready (PPO%s)",
                " + SHAP" if shap_ready else " only; SHAP unavailable",
            )
        else:
            logger.warning("AgentSingleton: SchedulerAgent.load returned None — heuristic mode")

    except Exception as exc:
        logger.error("AgentSingleton: agent load failed: %s", exc, exc_info=True)

    try:
        from ai_engine.kafka.producer import CloudOSProducer

        producer = CloudOSProducer(config)

        with _lock:
            _producer = producer

        logger.info("AgentSingleton: KafkaProducer ready")

    except Exception as exc:
        logger.warning("AgentSingleton: Kafka unavailable (%s) — decisions won't publish", exc)

    with _lock:
        _ready = True

    logger.info("AgentSingleton: initialisation complete")


def startup_initialise() -> None:
    """
    Called from FastAPI lifespan on startup — runs in background thread.
    """
    t = threading.Thread(target=_initialise, daemon=True, name="agent-init")
    t.start()


def get_agent() -> Optional[object]:
    """
    FastAPI dependency — returns agent or None if still loading / failed.
    """
    with _lock:
        return _agent


def get_producer() -> Optional[object]:
    """
    FastAPI dependency — returns producer or None.
    """
    with _lock:
        return _producer


def is_ready() -> bool:
    """
    Returns True once initialisation attempt has completed.
    """
    with _lock:
        return _ready