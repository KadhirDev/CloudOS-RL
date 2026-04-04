# =============================================================================
# CloudOS-RL Application Image
# All heavy dependencies pre-installed at build time.
# pip install never runs at pod startup.
# =============================================================================

FROM python:3.11-slim

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps — installed in layers for cache efficiency ───────────────────

# Layer 1: lightweight deps (fast, changes rarely)
RUN pip install --no-cache-dir \
        fastapi==0.111.0 \
        uvicorn[standard]==0.29.0 \
        pyyaml==6.0.1 \
        pydantic==2.7.1 \
        pydantic-settings==2.2.1 \
        boto3==1.34.0 \
        httpx==0.27.0 \
        requests==2.31.0 \
        prometheus-client==0.20.0 \
        confluent-kafka==2.3.0 \
        "python-jose[cryptography]"

# Layer 2: scientific stack
# Keep NumPy 2.x because the current model/vecnorm artifacts were loading
# successfully with NumPy 2 in your container, while NumPy 1.26.4 caused
# model deserialization failure in this project.
RUN pip install --no-cache-dir \
        "numpy>=2.0.0,<2.2.0" \
        scipy==1.13.0 \
        joblib==1.4.0

# Layer 3: ML stack (largest — own layer so it caches independently)
RUN pip install --no-cache-dir \
        torch==2.3.0+cpu \
        --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir \
        gymnasium==0.29.1 \
        stable-baselines3==2.3.0

# Layer 4: SHAP
# SHAP 0.46.0 adds NumPy 2 support; 0.45.0 was the incompatible version in logs.
RUN pip install --no-cache-dir \
        shap==0.46.0

RUN pip install --no-cache-dir passlib[bcrypt] bcrypt==3.2.2

# ── Application code ──────────────────────────────────────────────────────────
WORKDIR /app
COPY . .

# Create required runtime directories
RUN mkdir -p data/pricing data/carbon data/shap models/best models/checkpoints

# ── Runtime ───────────────────────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

EXPOSE 8000
EXPOSE 9090

# Default command (overridden per deployment)
CMD ["python", "-m", "uvicorn", "backend.api.main:app", \
     "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]