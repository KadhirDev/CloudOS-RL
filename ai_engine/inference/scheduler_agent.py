"""
SchedulerAgent
===============
Loads the trained PPO model and runs inference.
Integrates SHAP explainability into every decision when available.

Decision pipeline per request:
  WorkloadRequest
       ↓
  build_state()           → float32 observation vector
       ↓
  VecNormalize.normalize  → optional observation normalization
       ↓
  PPO.predict()           → MultiDiscrete action
       ↓
  ActionDecoder.decode()  → Cloud decision dict / object
       ↓
  SHAPExplainer.explain() → optional attributions
       ↓
  ExplanationFormatter    → optional human-readable explanation
       ↓
  SchedulingDecision      → returned to API + Kafka producer

SHAP is optional — if the model is not trained yet or the background dataset
is missing, the agent still returns decisions without explanations.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import pickle
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

_MODEL_PATH = Path("models/best/best_model")
_VECNORM_PATH = Path("models/vec_normalize.pkl")


class SchedulerAgent:
    """
    Inference wrapper for the trained PPO scheduling agent.
    Loads model once, serves many requests, with optional SHAP explanations.
    """

    def __init__(
        self,
        model: Any,
        vec_env: Any,
        config: Dict[str, Any],
        explainer: Any = None,
        formatter: Any = None,
    ) -> None:
        self._model = model
        self._vec_env = vec_env
        self._config = config or {}
        self._explainer = explainer
        self._formatter = formatter

        from ai_engine.environment.action_decoder import ActionDecoder
        from ai_engine.environment.state_builder import StateBuilder
        from ai_engine.cloud_adapter.pricing_cache import PricingCache

        self._decoder = ActionDecoder()
        self._state_builder = StateBuilder(self._config)
        self._pricing_cache = PricingCache(self._config)

    # -------------------------------------------------------------------------
    # Factory
    # -------------------------------------------------------------------------
    @classmethod
    def load(
        cls,
        config: Dict[str, Any],
        model_path: Optional[str] = None,
        vecnorm_path: Optional[str] = None,
        with_explainer: bool = True,
        force_bg_regen: bool = False,
    ) -> Optional["SchedulerAgent"]:
        """
        Load PPO model + optional VecNormalize + optional SHAP explainer.

        Path resolution priority:
          1) explicit function args
          2) environment variables
          3) config dict
          4) built-in defaults
        """
        from stable_baselines3 import PPO
        from stable_baselines3.common.vec_env import DummyVecEnv

        mp_str = (
            model_path
            or os.environ.get("CLOUDOS_MODEL_PATH", "")
            or config.get("model", {}).get("path", str(_MODEL_PATH))
        )
        vp_str = (
            vecnorm_path
            or os.environ.get("CLOUDOS_VECNORM_PATH", "")
            or config.get("model", {}).get("vecnorm", str(_VECNORM_PATH))
        )

        mp = Path(mp_str)
        vp = Path(vp_str)
        model_file = mp if mp.suffix else Path(f"{mp}.zip")

        logger.info(
            "SchedulerAgent.load: model_file=%s exists=%s",
            model_file,
            model_file.exists(),
        )
        logger.info(
            "SchedulerAgent.load: vecnorm=%s exists=%s",
            vp,
            vp.exists(),
        )

        if not model_file.exists():
            logger.error(
                "SchedulerAgent: model file not found: %s\n"
                "  Files in parent dir: %s",
                model_file,
                list(model_file.parent.glob("*")) if model_file.parent.exists() else "dir missing",
            )
            return None

        try:
            custom_objects = {
                "learning_rate": 0.0003,
                "lr_schedule": lambda _: 0.0003,
                "clip_range": lambda _: 0.2,
            }
            model = PPO.load(
                str(model_file),
                device="cpu",
                custom_objects=custom_objects,
            )
            logger.info("SchedulerAgent: PPO loaded from %s", model_file)
        except Exception as exc:
            logger.error("SchedulerAgent: model load failed: %s", exc, exc_info=True)
            return None

        vec_env = None
        if vp.exists():
            try:
                from ai_engine.environment.cloud_env import CloudOSEnv

                dummy = DummyVecEnv([lambda: CloudOSEnv(config)])

                cls._install_numpy_pickle_compat()

                with open(vp, "rb") as fh:
                    vec_env = pickle.load(fh)

                if hasattr(vec_env, "set_venv"):
                    vec_env.set_venv(dummy)
                    vec_env.training = False
                    vec_env.norm_reward = False
                    logger.info("SchedulerAgent: VecNormalize loaded from %s", vp)
                else:
                    logger.warning(
                        "SchedulerAgent: unpickled vecnorm has no set_venv(); running unnormalised."
                    )
                    vec_env = None

            except Exception as exc:
                logger.warning(
                    "SchedulerAgent: VecNormalize load failed (%s) — running unnormalised.",
                    exc,
                    exc_info=True,
                )
                vec_env = None
        else:
            logger.warning("SchedulerAgent: %s not found — running unnormalised.", vp)

        explainer = None
        formatter = None

        if with_explainer:
            try:
                from ai_engine.explainability.shap_explainer import SHAPExplainer
                from ai_engine.explainability.explanation_formatter import ExplanationFormatter

                if hasattr(SHAPExplainer, "load"):
                    explainer = SHAPExplainer.load(
                        model=model,
                        config=config,
                        nsamples=100,
                        force_regen=force_bg_regen,
                    )
                else:
                    explainer = SHAPExplainer(
                        model=model,
                        config=config,
                        nsamples=100,
                        force_regen=force_bg_regen,
                    )

                formatter = ExplanationFormatter()
                logger.info("SchedulerAgent: SHAP explainer ready")

            except Exception as exc:
                logger.warning(
                    "SchedulerAgent: SHAP init failed (%s) — continuing without explanations.",
                    exc,
                )
                explainer = None
                formatter = None

        return cls(
            model=model,
            vec_env=vec_env,
            config=config,
            explainer=explainer,
            formatter=formatter,
        )

    @staticmethod
    def _install_numpy_pickle_compat() -> None:
        """
        Install compatibility aliases for NumPy pickle module paths.

        Handles both directions:
        - numpy 1.x pickles referring to numpy.core.*
        - numpy 2.x pickles referring to numpy._core.*
        """
        try:
            import numpy.core as _np_core  # noqa: F401
        except ImportError:
            import numpy._core as _np__core  # type: ignore

            sys.modules.setdefault("numpy.core", _np__core)
            sys.modules.setdefault(
                "numpy.core.numeric",
                importlib.import_module("numpy._core.numeric"),
            )
            sys.modules.setdefault(
                "numpy.core.multiarray",
                importlib.import_module("numpy._core.multiarray"),
            )

        try:
            import numpy._core as _np__core  # noqa: F401
        except ImportError:
            import numpy.core as _np_core  # type: ignore

            sys.modules.setdefault("numpy._core", _np_core)
            sys.modules.setdefault(
                "numpy._core.numeric",
                importlib.import_module("numpy.core.numeric"),
            )
            sys.modules.setdefault(
                "numpy._core.multiarray",
                importlib.import_module("numpy.core.multiarray"),
            )

    # -------------------------------------------------------------------------
    # Public status helpers
    # -------------------------------------------------------------------------
    @property
    def model(self) -> Any:
        return self._model

    @property
    def vec_env(self) -> Any:
        return self._vec_env

    @property
    def explainer(self) -> Any:
        return self._explainer

    @property
    def formatter(self) -> Any:
        return self._formatter

    def is_model_ready(self) -> bool:
        return self._model is not None

    def has_explainer(self) -> bool:
        return self._explainer is not None

    # -------------------------------------------------------------------------
    # Core inference
    # -------------------------------------------------------------------------
    def schedule(
        self,
        workload: Any,
        include_explanation: bool = True,
    ) -> Dict[str, Any]:
        """
        Main inference entrypoint.
        Accepts either a dict-like workload or a request model / dataclass object.
        """
        started = time.perf_counter()

        workload_dict = self._to_dict(workload)
        logger.debug("SchedulerAgent.schedule: workload=%s", workload_dict)

        state = self._build_state(workload_dict)
        state = self._ensure_state_array(state)

        norm_state = self._normalise_obs(state)
        action, _raw_action = self._predict(norm_state)
        decoded = self._decode_action(action, workload, workload_dict)

        explanation = None
        if include_explanation:
            explanation = self._build_explanation(
                raw_state=state,
                norm_state=norm_state,
                action=action,
                decoded=decoded,
                workload=workload,
                workload_dict=workload_dict,
            )

        duration_ms = round((time.perf_counter() - started) * 1000.0, 3)

        result = self._merge_decision_output(
            decoded=decoded,
            workload_dict=workload_dict,
            action=action,
            explanation=explanation,
            duration_ms=duration_ms,
        )

        logger.info(
            "SchedulerAgent.schedule: completed in %.3f ms (explainer=%s)",
            duration_ms,
            self._explainer is not None,
        )
        return result

    def decide(
        self,
        workload: Any,
        include_explanation: bool = True,
    ) -> Dict[str, Any]:
        return self.schedule(workload=workload, include_explanation=include_explanation)

    def predict_decision(
        self,
        workload: Any,
        include_explanation: bool = True,
    ) -> Dict[str, Any]:
        return self.schedule(workload=workload, include_explanation=include_explanation)

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------
    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}

        if isinstance(obj, dict):
            return dict(obj)

        if hasattr(obj, "model_dump"):
            try:
                return obj.model_dump()
            except Exception:
                pass

        if hasattr(obj, "dict"):
            try:
                return obj.dict()
            except Exception:
                pass

        if hasattr(obj, "__dict__"):
            try:
                return {k: v for k, v in vars(obj).items() if not k.startswith("_")}
            except Exception:
                pass

        return {"value": obj}

    def _get_pricing(self) -> Dict[str, Any]:
        pricing: Dict[str, Any] = {}

        if self._pricing_cache is None:
            return pricing

        for method_name in ("get_current_pricing", "get_pricing"):
            method = getattr(self._pricing_cache, method_name, None)
            if callable(method):
                try:
                    pricing = method() or {}
                    break
                except Exception as exc:
                    logger.debug("_get_pricing: %s failed (%s)", method_name, exc)

        return pricing

    def _load_carbon(self) -> Dict[str, Any]:
        """
        Best-effort carbon data loader.
        """
        carbon: Dict[str, Any] = {}

        path = (
            self._config.get("data_pipeline", {}).get("carbon_output_path")
            or self._config.get("environment", {}).get("carbon_output_path")
            or "data/carbon/carbon_intensity.json"
        )

        try:
            carbon_path = Path(path)
            if carbon_path.exists():
                with open(carbon_path, "r", encoding="utf-8") as fh:
                    carbon = json.load(fh) or {}
        except Exception as exc:
            logger.debug("_load_carbon: failed to load carbon file (%s)", exc)

        return carbon

    def build_state(self, workload: Dict[str, Any]) -> np.ndarray:
        """
        Public entry point — builds 45-dim state from workload dict.
        """
        return self._build_state(workload)

    def _build_state(self, workload: Dict[str, Any]) -> np.ndarray:
        """
        Calls StateBuilder.build(workload, pricing, carbon, history).
        Falls back carefully across older method signatures.
        """
        workload_dict = self._to_dict(workload)

        if "cpu_request_vcpu" in workload_dict and "cpu_request" not in workload_dict:
            workload_dict["cpu_request"] = workload_dict["cpu_request_vcpu"]

        pricing = self._get_pricing()
        carbon = self._load_carbon()
        history: List[Any] = []

        if self._state_builder is not None:
            try:
                state = self._state_builder.build(workload_dict, pricing, carbon, history)
                if isinstance(state, np.ndarray) and state.shape == (45,):
                    logger.info("SchedulerAgent._build_state: using correct build(...) signature")
                    return state.astype(np.float32)

                if isinstance(state, np.ndarray):
                    logger.warning(
                        "_build_state: build() returned shape %s, expected (45,)",
                        state.shape,
                    )
                    return state.astype(np.float32)

            except TypeError as exc:
                logger.warning(
                    "_build_state: build(workload, pricing, carbon, history) failed (%s) — trying fallbacks",
                    exc,
                )
            except Exception as exc:
                logger.error("SchedulerAgent._build_state: build() failed: %s", exc, exc_info=True)
                raise

            try:
                state = self._state_builder.build(workload_dict)
                if isinstance(state, np.ndarray):
                    logger.info("SchedulerAgent._build_state: using fallback build(workload)")
                    return state.astype(np.float32)
            except TypeError:
                pass
            except Exception as exc:
                logger.warning("_build_state: fallback build(workload) failed (%s)", exc)

            for method_name in ("build_state", "from_workload", "get_state"):
                method = getattr(self._state_builder, method_name, None)
                if callable(method):
                    try:
                        state = method(workload_dict)
                        if isinstance(state, np.ndarray):
                            logger.info("SchedulerAgent._build_state: using fallback %s(...)", method_name)
                            return state.astype(np.float32)
                    except Exception:
                        continue

        logger.error(
            "SchedulerAgent: no compatible StateBuilder method found. Available methods: %s",
            [m for m in dir(self._state_builder) if not m.startswith("_")]
            if self._state_builder
            else "state_builder=None",
        )
        return np.zeros(45, dtype=np.float32)

    def _ensure_state_array(self, state: Any) -> np.ndarray:
        arr = np.asarray(state, dtype=np.float32)
        if arr.ndim > 1:
            arr = arr.reshape(-1)
        return arr

    def _normalise_obs(self, state: np.ndarray) -> np.ndarray:
        if self._vec_env is None:
            return state

        try:
            batched = np.asarray([state], dtype=np.float32)
            normed = self._vec_env.normalize_obs(batched)

            if isinstance(normed, np.ndarray):
                if normed.ndim >= 2:
                    return normed[0].astype(np.float32)
                return normed.astype(np.float32)

            return state
        except Exception as exc:
            logger.warning(
                "SchedulerAgent: observation normalization failed (%s) — using raw state.",
                exc,
                exc_info=True,
            )
            return state

    def _predict(self, obs: np.ndarray) -> Tuple[List[int], Any]:
        if self._model is None:
            raise RuntimeError("SchedulerAgent: model not loaded")

        try:
            action, state = self._model.predict(obs, deterministic=True)
        except Exception as exc:
            logger.error("SchedulerAgent: PPO.predict failed: %s", exc, exc_info=True)
            raise

        action_list = self._as_action_list(action)
        return action_list, state

    def _as_action_list(self, action: Any) -> List[int]:
        if isinstance(action, np.ndarray):
            flat = action.reshape(-1).tolist()
            return [int(x) for x in flat]

        if isinstance(action, (list, tuple)):
            return [int(x) for x in action]

        return [int(action)]

    def _decode_action(
        self,
        action: List[int],
        workload: Any,
        workload_dict: Dict[str, Any],
    ) -> Any:
        candidates = [
            ("decode", (action,)),
            ("decode", (action, workload)),
            ("decode", (action, workload_dict)),
            ("decode_action", (action,)),
            ("decode_action", (action, workload)),
            ("decode_action", (action, workload_dict)),
        ]

        for method_name, args in candidates:
            method = getattr(self._decoder, method_name, None)
            if callable(method):
                try:
                    decoded = method(*args)
                    logger.debug(
                        "SchedulerAgent._decode_action: used ActionDecoder.%s",
                        method_name,
                    )
                    return decoded
                except TypeError:
                    continue

        logger.warning(
            "SchedulerAgent: no compatible ActionDecoder method found; returning raw action wrapper."
        )
        return {"action": action}

    def _build_explanation(
        self,
        raw_state: np.ndarray,
        norm_state: np.ndarray,
        action: List[int],
        decoded: Any,
        workload: Any,
        workload_dict: Dict[str, Any],
    ) -> Optional[Any]:
        if self._explainer is None:
            return None

        shap_result = None

        explain_candidates = [
            ("explain", (), {"state": norm_state}),
            ("explain", (), {"state": raw_state}),
            ("explain", (), {"observation": norm_state}),
            ("explain", (), {"observation": raw_state}),
            ("explain", (), {"obs": norm_state}),
            ("explain", (), {"obs": raw_state}),
            ("explain", (norm_state,), {}),
            ("explain", (raw_state,), {}),
        ]

        for method_name, args, kwargs in explain_candidates:
            method = getattr(self._explainer, method_name, None)
            if callable(method):
                try:
                    shap_result = method(*args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception as exc:
                    logger.warning(
                        "SchedulerAgent: explainer.%s failed (%s)",
                        method_name,
                        exc,
                        exc_info=True,
                    )
                    shap_result = None
                    break

        if shap_result is None:
            return None

        if self._formatter is None:
            return shap_result

        format_candidates = [
            ("format", (), {"explanation": shap_result, "decision": decoded, "workload": workload_dict}),
            ("format", (), {"explanation": shap_result, "decision": decoded}),
            ("format", (shap_result,), {}),
        ]

        for method_name, args, kwargs in format_candidates:
            method = getattr(self._formatter, method_name, None)
            if callable(method):
                try:
                    return method(*args, **kwargs)
                except TypeError:
                    continue
                except Exception as exc:
                    logger.warning(
                        "SchedulerAgent: formatter.%s failed (%s)",
                        method_name,
                        exc,
                        exc_info=True,
                    )
                    return shap_result

        return shap_result

    def _merge_decision_output(
        self,
        decoded: Any,
        workload_dict: Dict[str, Any],
        action: List[int],
        explanation: Any,
        duration_ms: float,
    ) -> Dict[str, Any]:
        """
        Normalize the final result into a dict that API / Kafka can serialize.
        """
        base: Dict[str, Any]

        if isinstance(decoded, dict):
            base = dict(decoded)
        elif hasattr(decoded, "model_dump"):
            try:
                base = decoded.model_dump()
            except Exception:
                base = {"decision": str(decoded)}
        elif hasattr(decoded, "dict"):
            try:
                base = decoded.dict()
            except Exception:
                base = {"decision": str(decoded)}
        elif hasattr(decoded, "__dict__"):
            try:
                base = {k: v for k, v in vars(decoded).items() if not k.startswith("_")}
            except Exception:
                base = {"decision": str(decoded)}
        else:
            base = {"decision": decoded}

        base.setdefault("workload", workload_dict)
        base.setdefault("action", action)
        base["inference_ms"] = duration_ms

        if explanation is not None:
            base["explanation"] = explanation

        return base

    # -------------------------------------------------------------------------
    # Convenience API
    # -------------------------------------------------------------------------
    def warmup(self) -> None:
        try:
            dummy = np.zeros(45, dtype=np.float32)
            _ = self._normalise_obs(dummy)
            logger.info("SchedulerAgent: warmup complete")
        except Exception as exc:
            logger.warning("SchedulerAgent: warmup skipped (%s)", exc)

    def status(self) -> Dict[str, Any]:
        return {
            "agent_loaded": self._model is not None,
            "vecnorm_loaded": self._vec_env is not None,
            "shap_ready": self._explainer is not None,
            "model_type": type(self._model).__name__ if self._model is not None else None,
        }