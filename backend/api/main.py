"""
CloudOS-RL FastAPI Application
================================
Entry point for the scheduling API.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.api.routes.scheduling import router as scheduling_router
from backend.auth.router import router as auth_router
from backend.core.agent_singleton import get_agent, is_ready, startup_initialise

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-40s  %(levelname)s  %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: load agent in background. Shutdown: flush Kafka."""
    logger.info("CloudOS-RL API starting — loading RL agent in background ...")
    startup_initialise()
    yield
    logger.info("CloudOS-RL API shutting down.")


app = FastAPI(
    title="CloudOS-RL Scheduling API",
    description="AI-native multi-cloud workload scheduler powered by PPO reinforcement learning.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────────────────────
app.include_router(scheduling_router)
app.include_router(auth_router)


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["health"])
async def health():
    agent = get_agent()
    return {
        "status": "ok",
        "agent_loaded": agent is not None,
        "shap_ready": agent._explainer is not None if agent else False,
        "ready": is_ready(),
    }


@app.get("/", tags=["health"])
async def root():
    return {"service": "cloudos-rl-api", "version": "1.0.0"}