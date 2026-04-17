"""
SHAP Explainer
===============
Wraps the PPO critic (value function) with a SHAP KernelExplainer
to produce per-feature attributions for each scheduling decision.

How it works:
  1. PPO predicts actions via the policy network.
  2. SHAP attributes the critic's VALUE ESTIMATE to individual features.
     (The value estimate is a scalar proxy for expected future reward.)
  3. Top-5 positive and top-5 negative drivers are extracted.
  4. These are embedded in the decision payload sent to Kafka → Grafana.

SHAP KernelExplainer:
  - Model-agnostic: works with any black-box function
  - Background dataset: generated via BackgroundDataGenerator
  - Explains one sample at a time (nsamples=100 default, capped lower in container-safe load)
  - Each explanation call: CPU-only and optional

To avoid re-initialising on every request:
  - SHAPExplainer is loaded once and attached by SchedulerAgent / AgentSingleton
  - Background dataset is loaded from a writable cache path inside containers

Compatible with:
  - stable-baselines3 PPO policy (MlpPolicy)
  - ai_engine/inference/scheduler_agent.py
  - ai_engine/explainability/background_generator.py
  - ai_engine/explainability/explanation_formatter.py
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

try:
    import shap
except ImportError:
    raise ImportError(
        "\n\nshap is not installed.\n"
        "Fix: run   pip install shap   then try again.\n"
    )

from ai_engine.explainability.background_generator import (
    FEATURE_NAMES,
    BackgroundDataGenerator,
)

logger = logging.getLogger(__name__)

# Default local path retained for compatibility, though container-safe load()
# uses /tmp/shap unless SHAP_CACHE_DIR is provided.
_BG_PATH = Path("data/shap/background_dataset.npy")


class SHAPExplainer:
    """
    SHAP KernelExplainer wrapping the PPO critic value function.

    Usage:
        explainer = SHAPExplainer.load(model, config)
        if explainer is not None:
            explanation = explainer.explain(state_45dim)
    """

    def __init__(
        self,
        model,
        background: np.ndarray,
        nsamples: int = 100,
    ):
        """
        Args:
            model:      Stable-Baselines3 PPO model (or compatible).
            background: (n, 45) float32 background dataset.
            nsamples:   SHAP samples per explanation call.
                        Higher = more accurate, slower.
        """
        self._model = model
        self._background = np.asarray(background, dtype=np.float32)
        self._nsamples = int(nsamples)
        self._explainer: Optional[shap.KernelExplainer] = None
        self._feature_names = FEATURE_NAMES

        self._initialise_explainer()

    # -----------------------------------------------------------------------
    # Factory
    # -----------------------------------------------------------------------

    @classmethod
    def load(
        cls,
        model,
        config: Dict,
        nsamples: int = 100,
        force_regen: bool = False,
    ) -> Optional["SHAPExplainer"]:
        """
        Loads or generates background dataset and returns a SHAPExplainer.
        Uses /tmp/shap inside containers (always writable by default).
        Returns None on any failure — caller must handle None safely.

        Args:
            model:       SB3 PPO model.
            config:      Project config dict.
            nsamples:    SHAP samples per explanation call.
            force_regen: If True, regenerate background dataset even if cached.
        """
        logger.info("SHAPExplainer.load: preparing background dataset ...")

        shap_dir = Path(os.environ.get("SHAP_CACHE_DIR", "/tmp/shap"))
        try:
            shap_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning(
                "SHAPExplainer.load: failed to create SHAP cache dir %s (%s)",
                shap_dir,
                exc,
            )
            return None

        bg_path = shap_dir / "background_dataset.npy"
        meta_path = shap_dir / "background_metadata.json"

        gen = BackgroundDataGenerator(config)

        # Monkey-patch generator module paths for this invocation only.
        import ai_engine.explainability.background_generator as _bgmod

        _orig_output = getattr(_bgmod, "_BG_OUTPUT_PATH", None)
        _orig_meta = getattr(_bgmod, "_META_PATH", None)
        _orig_dir = getattr(_bgmod, "_BG_OUTPUT_DIR", None)

        background = None

        try:
            _bgmod._BG_OUTPUT_DIR = shap_dir
            _bgmod._BG_OUTPUT_PATH = bg_path
            _bgmod._META_PATH = meta_path

            # Keep init lighter inside containers / startup paths.
            n = min(int(nsamples), 50)
            if n <= 0:
                n = 50

            logger.info(
                "SHAPExplainer.load: generating/loading background (n=%d) at %s",
                n,
                bg_path,
            )
            background = gen.generate(
                n_samples=n,
                seed=42,
                force=force_regen,
            )

        except Exception as exc:
            logger.warning(
                "SHAPExplainer.load: background generation failed (%s)",
                exc,
            )
            background = None

        finally:
            if _orig_output is not None:
                _bgmod._BG_OUTPUT_PATH = _orig_output
            if _orig_meta is not None:
                _bgmod._META_PATH = _orig_meta
            if _orig_dir is not None:
                _bgmod._BG_OUTPUT_DIR = _orig_dir

        if background is None:
            logger.warning("SHAPExplainer.load: no background data — SHAP disabled")
            return None

        try:
            background = np.asarray(background, dtype=np.float32)
        except Exception as exc:
            logger.warning(
                "SHAPExplainer.load: invalid background array (%s) — SHAP disabled",
                exc,
            )
            return None

        if background.size == 0:
            logger.warning("SHAPExplainer.load: empty background data — SHAP disabled")
            return None

        if background.ndim != 2:
            logger.warning(
                "SHAPExplainer.load: expected 2D background, got shape %s — SHAP disabled",
                getattr(background, "shape", None),
            )
            return None

        expected_dim = len(FEATURE_NAMES)
        if background.shape[1] != expected_dim:
            logger.warning(
                "SHAPExplainer.load: background feature width mismatch "
                "(got %d expected %d) — SHAP disabled",
                background.shape[1],
                expected_dim,
            )
            return None

        logger.info("SHAPExplainer.load: background shape %s", background.shape)

        try:
            return cls(model, background, nsamples=min(int(nsamples), 50))
        except Exception as exc:
            logger.warning(
                "SHAPExplainer.load: init failed (%s) — SHAP disabled",
                exc,
            )
            return None

    # -----------------------------------------------------------------------
    # Public
    # -----------------------------------------------------------------------

    def explain(self, state: np.ndarray) -> Dict:
        """
        Computes SHAP values for a single 45-dim state vector.

        Args:
            state: (45,) or (1, 45) float32 array.

        Returns:
            {
              "top_drivers":        [{"feature": str, "shap_value": float, "direction": str}, ...],
              "base_value":         float,
              "shap_values":        {feature_name: shap_value, ...},
              "top_positive":       [{"feature": str, "shap_value": float}, ...],
              "top_negative":       [{"feature": str, "shap_value": float}, ...],
              "explanation_ms":     float,
              "state_mean":         float,
              "state_std":          float,
            }
        """
        if self._explainer is None:
            logger.error("SHAPExplainer: explainer not initialised — returning empty.")
            return self._empty_explanation()

        try:
            x = np.array(state, dtype=np.float32).reshape(1, len(self._feature_names))
        except Exception as exc:
            logger.error("SHAPExplainer: invalid state input: %s", exc)
            return self._empty_explanation(error="invalid_state")

        state_mean = round(float(x.mean()), 4)
        state_std = round(float(x.std()), 4)

        t0 = time.perf_counter()
        try:
            shap_vals = self._explainer.shap_values(
                x,
                nsamples=self._nsamples,
                silent=True,
            )
        except Exception as exc:
            logger.error("SHAPExplainer: shap_values failed: %s", exc)
            elapsed_ms = max(0.01, (time.perf_counter() - t0) * 1000)
            return self._empty_explanation(
                error="shap_values_failed",
                explanation_ms=elapsed_ms,
                state_mean=state_mean,
                state_std=state_std,
            )

        elapsed_ms = max(0.01, (time.perf_counter() - t0) * 1000)

        # KernelExplainer may return list or ndarray depending on SHAP version.
        try:
            if isinstance(shap_vals, list):
                vals = np.array(shap_vals[0], dtype=np.float32).reshape(-1)
            else:
                vals = np.array(shap_vals, dtype=np.float32).reshape(-1)
        except Exception as exc:
            logger.error("SHAPExplainer: failed to parse shap values: %s", exc)
            return self._empty_explanation(
                error="shap_parse_failed",
                explanation_ms=elapsed_ms,
                state_mean=state_mean,
                state_std=state_std,
            )

        try:
            expected_value = self._explainer.expected_value
            if isinstance(expected_value, (list, tuple, np.ndarray)):
                base_val = float(np.array(expected_value).flat[0])
            else:
                base_val = float(expected_value)
        except Exception:
            base_val = 0.0

        named: Dict[str, float] = {
            name: round(float(v), 6)
            for name, v in zip(self._feature_names, vals)
        }

        # Use an absolute threshold instead of strict > 0 / < 0.
        # This avoids clutter from tiny numerical noise while still preserving
        # meaningful small signals.
        all_abs = [abs(v) for v in vals]
        total_abs = sum(all_abs)
        threshold = total_abs * 0.01 if total_abs > 1e-9 else 1e-9

        sorted_desc = sorted(named.items(), key=lambda kv: kv[1], reverse=True)
        sorted_asc = sorted(named.items(), key=lambda kv: kv[1])

        top_positive = [
            {"feature": k, "shap_value": round(v, 6)}
            for k, v in sorted_desc[:5]
            if v > threshold
        ]

        top_negative = [
            {"feature": k, "shap_value": round(v, 6)}
            for k, v in sorted_asc[:5]
            if v < -threshold
        ]

        top_drivers = sorted(
            [
                {
                    "feature": k,
                    "shap_value": round(v, 6),
                    "direction": "positive" if v >= 0 else "negative",
                }
                for k, v in named.items()
            ],
            key=lambda d: abs(d["shap_value"]),
            reverse=True,
        )[:5]

        logger.debug(
            "SHAP: %.1fms | base=%.3f | top=%s",
            elapsed_ms,
            base_val,
            top_drivers[0]["feature"] if top_drivers else "none",
        )

        return {
            "top_drivers": top_drivers,
            "base_value": round(base_val, 6),
            "shap_values": named,
            "top_positive": top_positive,
            "top_negative": top_negative,
            "explanation_ms": round(elapsed_ms, 2),
            "state_mean": state_mean,
            "state_std": state_std,
        }

    def get_feature_names(self) -> List[str]:
        return list(self._feature_names)

    def get_background_shape(self) -> tuple:
        return tuple(self._background.shape)

    # -----------------------------------------------------------------------
    # Private
    # -----------------------------------------------------------------------

    def _initialise_explainer(self):
        """Creates the SHAP KernelExplainer wrapping the PPO critic."""
        logger.info(
            "SHAPExplainer: initialising KernelExplainer "
            "(background=%s, nsamples=%d) ...",
            self._background.shape,
            self._nsamples,
        )
        t0 = time.perf_counter()

        try:
            def _critic_fn(x: np.ndarray) -> np.ndarray:
                """
                Wrapper around PPO critic value function.
                Input:  (n, 45) float32
                Output: (n,)    float32 — value estimates
                """
                import torch

                x = np.asarray(x, dtype=np.float32)
                if x.ndim == 1:
                    x = x.reshape(1, -1)

                x_t = torch.tensor(x, dtype=torch.float32)
                vals = []

                with torch.no_grad():
                    for i in range(len(x_t)):
                        obs = x_t[i].unsqueeze(0)
                        try:
                            v = self._model.policy.predict_values(obs)
                            vals.append(float(v.item()))
                        except Exception:
                            vals.append(0.0)

                return np.array(vals, dtype=np.float32)

            self._explainer = shap.KernelExplainer(
                model=_critic_fn,
                data=self._background,
            )

            elapsed = (time.perf_counter() - t0) * 1000
            logger.info(
                "SHAPExplainer: KernelExplainer ready in %.0f ms",
                elapsed,
            )

        except Exception as exc:
            logger.error("SHAPExplainer: initialisation failed: %s", exc)
            self._explainer = None

    @staticmethod
    def _empty_explanation(
        error: str = "explainer_not_ready",
        explanation_ms: float = 0.0,
        state_mean: float = 0.0,
        state_std: float = 0.0,
    ) -> Dict:
        return {
            "top_drivers": [],
            "base_value": 0.0,
            "shap_values": {},
            "top_positive": [],
            "top_negative": [],
            "explanation_ms": round(float(explanation_ms), 2),
            "state_mean": state_mean,
            "state_std": state_std,
            "error": error,
        }