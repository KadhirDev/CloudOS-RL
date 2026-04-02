"""
PricingCache
=============
Provides EC2 pricing data to the rest of the system with this priority chain:

  Priority 1: data/pricing/aws_pricing.json   <- written by DataPipelineOrchestrator
  Priority 2: AWS Pricing API direct call      <- if pipeline file missing
  Priority 3: Hardcoded fallback constants     <- always available, zero network

Optimized safely for concurrency:
  - Thread-safe in-memory TTL cache
  - Stampede-safe refresh: only one thread refreshes on expiry
  - File-mtime detection: reload immediately if pipeline writes a newer file
  - Flat pricing cache so repeat callers do not re-flatten on every request
  - Preserves existing public API and behavior
"""

import json
import logging
import threading
import time
from pathlib import Path
from typing import Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# Refresh pricing data in memory at most once per minute unless file mtime changes.
_PRICING_CACHE_TTL_SEC = 60.0


# ---------------------------------------------------------------------------
# Hardcoded fallback — zero dependency, always available
# ---------------------------------------------------------------------------

_FALLBACK: Dict = {
    "us-east-1": {
        "t3.medium": 0.0416,
        "t3.large": 0.0832,
        "m5.large": 0.0960,
        "m5.xlarge": 0.1920,
        "c5.large": 0.0850,
        "c5.xlarge": 0.1700,
        "r5.large": 0.1260,
        "r5.xlarge": 0.2520,
        "g4dn.xlarge": 0.5260,
        "p3.2xlarge": 3.0600,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.65,
    },
    "us-east-2": {
        "m5.large": 0.096,
        "c5.large": 0.085,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.65,
    },
    "us-west-1": {
        "m5.large": 0.112,
        "c5.large": 0.096,
        "on_demand_per_vcpu_hr": 0.056,
        "spot_discount": 0.65,
    },
    "us-west-2": {
        "m5.large": 0.096,
        "c5.large": 0.085,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.68,
    },
    "eu-west-1": {
        "m5.large": 0.107,
        "c5.large": 0.097,
        "on_demand_per_vcpu_hr": 0.054,
        "spot_discount": 0.63,
    },
    "eu-west-2": {
        "m5.large": 0.111,
        "c5.large": 0.100,
        "on_demand_per_vcpu_hr": 0.056,
        "spot_discount": 0.65,
    },
    "eu-west-3": {
        "m5.large": 0.111,
        "c5.large": 0.100,
        "on_demand_per_vcpu_hr": 0.056,
        "spot_discount": 0.65,
    },
    "eu-central-1": {
        "m5.large": 0.111,
        "c5.large": 0.099,
        "on_demand_per_vcpu_hr": 0.056,
        "spot_discount": 0.65,
    },
    "eu-north-1": {
        "m5.large": 0.096,
        "c5.large": 0.086,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.70,
    },
    "ap-southeast-1": {
        "m5.large": 0.114,
        "c5.large": 0.100,
        "on_demand_per_vcpu_hr": 0.057,
        "spot_discount": 0.58,
    },
    "ap-southeast-2": {
        "m5.large": 0.122,
        "c5.large": 0.108,
        "on_demand_per_vcpu_hr": 0.061,
        "spot_discount": 0.65,
    },
    "ap-northeast-1": {
        "m5.large": 0.118,
        "c5.large": 0.107,
        "on_demand_per_vcpu_hr": 0.059,
        "spot_discount": 0.55,
    },
    "ap-northeast-2": {
        "m5.large": 0.114,
        "c5.large": 0.102,
        "on_demand_per_vcpu_hr": 0.057,
        "spot_discount": 0.65,
    },
    "ap-south-1": {
        "m5.large": 0.096,
        "c5.large": 0.085,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.65,
    },
    "ca-central-1": {
        "m5.large": 0.100,
        "c5.large": 0.090,
        "on_demand_per_vcpu_hr": 0.050,
        "spot_discount": 0.64,
    },
    "sa-east-1": {
        "m5.large": 0.142,
        "c5.large": 0.128,
        "on_demand_per_vcpu_hr": 0.071,
        "spot_discount": 0.65,
    },
    # GCP/Azure aliases used in ActionDecoder
    "us-central1": {
        "m5.large": 0.096,
        "c5.large": 0.085,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.65,
    },
    "europe-west4": {
        "m5.large": 0.107,
        "c5.large": 0.097,
        "on_demand_per_vcpu_hr": 0.054,
        "spot_discount": 0.60,
    },
    "eastus": {
        "m5.large": 0.096,
        "c5.large": 0.085,
        "on_demand_per_vcpu_hr": 0.048,
        "spot_discount": 0.65,
    },
    "westeurope": {
        "m5.large": 0.107,
        "c5.large": 0.097,
        "on_demand_per_vcpu_hr": 0.054,
        "spot_discount": 0.60,
    },
}

_LOCATION_MAP: Dict[str, str] = {
    "US East (N. Virginia)": "us-east-1",
    "US West (Oregon)": "us-west-2",
    "EU (Ireland)": "eu-west-1",
    "EU (Frankfurt)": "eu-central-1",
    "Asia Pacific (Singapore)": "ap-southeast-1",
    "Asia Pacific (Tokyo)": "ap-northeast-1",
    "Canada (Central)": "ca-central-1",
    "South America (Sao Paulo)": "sa-east-1",
}


class PricingCache:
    """
    Thread-safe pricing cache with TTL + file-mtime-based refresh.
    All public methods are safe to call from multiple threads.
    """

    def __init__(self, config: Dict):
        self._config = config or {}
        self._path = Path(
            self._config.get("pricing_fallback_path")
            or self._config.get("data_pipeline", {}).get(
                "pricing_output_path", "data/pricing/aws_pricing.json"
            )
        )

        self._lock = threading.Lock()

        # Cached raw nested pricing and flattened region->price map
        self._cached_raw: Optional[Dict] = None
        self._cached_flat: Optional[Dict[str, float]] = None

        # Monotonic timestamp for TTL checks
        self._cache_ts: float = 0.0

        # Last observed file mtime to allow immediate reload on fresh pipeline write
        self._file_mtime: float = 0.0

        # Stampede guard: only one thread refreshes on expiry
        self._refreshing: bool = False

    # -----------------------------------------------------------------------
    # Public
    # -----------------------------------------------------------------------

    def get_current_pricing(self) -> Dict[str, float]:
        """
        Returns flat pricing dict: {region: price_per_hr (float)}.
        Compatible with both StateBuilder (expects floats) and
        tests that iterate values as floats.

        Fast path:
          warm flat cache + TTL valid + no newer file

        Slow path:
          only one thread refreshes; others get stale cached flat pricing
          immediately instead of queueing behind file/API work.
        """
        now = time.monotonic()

        # Fast path without lock
        cached_flat = self._cached_flat
        if (
            cached_flat is not None
            and (now - self._cache_ts) <= _PRICING_CACHE_TTL_SEC
            and not self._is_file_newer()
        ):
            return dict(cached_flat)

        with self._lock:
            now = time.monotonic()

            if (
                self._cached_flat is not None
                and (now - self._cache_ts) <= _PRICING_CACHE_TTL_SEC
                and not self._has_newer_file_unlocked()
            ):
                return dict(self._cached_flat)

            if self._refreshing:
                if self._cached_flat is not None:
                    return dict(self._cached_flat)
                return dict(self._flatten_pricing(_FALLBACK))

            self._refreshing = True

        try:
            raw = self._refresh_from_sources()
            flat = self._flatten_pricing(raw)

            with self._lock:
                self._cached_raw = dict(raw)
                self._cached_flat = dict(flat)
                self._cache_ts = time.monotonic()
                try:
                    if self._path.exists():
                        self._file_mtime = self._path.stat().st_mtime
                except OSError:
                    pass
                self._refreshing = False

            return dict(flat)

        except Exception as exc:
            logger.warning("PricingCache: refresh failed (%s) — using last/stub fallback", exc)

            with self._lock:
                self._refreshing = False

                if self._cached_flat is not None:
                    return dict(self._cached_flat)

                fallback_flat = self._flatten_pricing(_FALLBACK)
                self._cached_raw = dict(_FALLBACK)
                self._cached_flat = dict(fallback_flat)
                self._cache_ts = time.monotonic()
                return dict(fallback_flat)

    def get_price(
        self,
        region: str,
        instance_type: str,
        purchase_option: str = "on_demand",
    ) -> float:
        """
        Returns price for region/instance/purchase combination.
        Uses raw pricing when available to read spot_discount and instance-level prices.
        """
        raw = self._load_raw_pricing()
        od_price = self._extract_on_demand_price(raw, region, instance_type)

        if purchase_option == "on_demand":
            return round(od_price, 6)

        region_data = raw.get(region, {})
        discount = 0.65
        if isinstance(region_data, dict):
            try:
                discount = float(region_data.get("spot_discount", 0.65))
            except (TypeError, ValueError):
                discount = 0.65

        if purchase_option == "spot":
            return round(od_price * (1.0 - discount), 6)
        if purchase_option == "reserved_1yr":
            return round(od_price * 0.60, 6)
        if purchase_option == "reserved_3yr":
            return round(od_price * 0.40, 6)
        if purchase_option == "savings_plan":
            return round(od_price * 0.55, 6)
        if purchase_option == "preemptible":
            return round(od_price * 0.30, 6)

        return round(od_price, 6)

    def invalidate(self) -> None:
        """Force the next pricing request to refresh from source."""
        with self._lock:
            self._cached_raw = None
            self._cached_flat = None
            self._cache_ts = 0.0
            self._file_mtime = 0.0
            self._refreshing = False

    # -----------------------------------------------------------------------
    # Private
    # -----------------------------------------------------------------------

    def _cache_expired(self) -> bool:
        return (time.monotonic() - self._cache_ts) > _PRICING_CACHE_TTL_SEC

    def _is_file_newer(self) -> bool:
        """
        Best-effort unlocked file freshness check.
        Safe for fast-path use; exact metadata coordination is rechecked under lock.
        """
        try:
            return self._path.exists() and self._path.stat().st_mtime > self._file_mtime
        except OSError:
            return False

    def _load_raw_pricing(self) -> Dict:
        """
        Loads raw pricing dict.

        Fast path:
          returns cached nested dict if TTL valid and file not newer.

        Slow path:
          only one thread refreshes from file -> AWS API -> fallback,
          while others get stale cached raw data immediately.
        """
        now = time.monotonic()

        cached_raw = self._cached_raw
        if (
            cached_raw is not None
            and (now - self._cache_ts) <= _PRICING_CACHE_TTL_SEC
            and not self._is_file_newer()
        ):
            return dict(cached_raw)

        with self._lock:
            now = time.monotonic()

            if (
                self._cached_raw is not None
                and (now - self._cache_ts) <= _PRICING_CACHE_TTL_SEC
                and not self._has_newer_file_unlocked()
            ):
                return dict(self._cached_raw)

            if self._refreshing:
                if self._cached_raw is not None:
                    return dict(self._cached_raw)
                return dict(_FALLBACK)

            self._refreshing = True

        try:
            raw = self._refresh_from_sources()
            flat = self._flatten_pricing(raw)

            with self._lock:
                self._cached_raw = dict(raw)
                self._cached_flat = dict(flat)
                self._cache_ts = time.monotonic()
                try:
                    if self._path.exists():
                        self._file_mtime = self._path.stat().st_mtime
                except OSError:
                    pass
                self._refreshing = False

            return dict(raw)

        except Exception as exc:
            logger.warning("PricingCache: raw refresh failed (%s) — using last/fallback", exc)

            with self._lock:
                self._refreshing = False

                if self._cached_raw is not None:
                    return dict(self._cached_raw)

                self._cached_raw = dict(_FALLBACK)
                self._cached_flat = self._flatten_pricing(_FALLBACK)
                self._cache_ts = time.monotonic()
                return dict(self._cached_raw)

    def _refresh_from_sources(self) -> Dict:
        """Refresh from pipeline file → AWS API → hardcoded fallback."""
        if self._path.exists():
            try:
                with open(self._path, encoding="utf-8") as fh:
                    data = json.load(fh)
                if data:
                    logger.debug("PricingCache: loaded from file %s", self._path)
                    return data
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("PricingCache: file read failed (%s) — trying AWS API", exc)

        try:
            api_data = self._fetch_from_aws()
            if api_data:
                self._persist_to_file(api_data)
                logger.info("PricingCache: loaded from AWS Pricing API")
                return api_data
        except (ClientError, BotoCoreError) as exc:
            logger.warning("PricingCache: AWS API unavailable (%s) — using fallback", exc)
        except Exception as exc:
            logger.warning("PricingCache: unexpected AWS pricing fetch failure (%s) — using fallback", exc)

        logger.info("PricingCache: using hardcoded fallback pricing")
        return dict(_FALLBACK)

    def _has_newer_file_unlocked(self) -> bool:
        """
        Returns True if the pipeline has written a newer file since last load.
        Caller must already hold self._lock if consistency with cache metadata matters.
        """
        try:
            return self._path.stat().st_mtime > self._file_mtime
        except OSError:
            return False

    @staticmethod
    def _extract_on_demand_price(raw: Dict, region: str, instance_type: str, default: float = 0.096) -> float:
        region_data = raw.get(region, {})
        if isinstance(region_data, (int, float)):
            return float(region_data)

        if isinstance(region_data, dict):
            try:
                # Prefer exact instance price when available
                if instance_type in region_data and isinstance(region_data[instance_type], (int, float)):
                    return float(region_data[instance_type])

                return float(
                    region_data.get("on_demand_per_vcpu_hr")
                    or region_data.get("m5.large")
                    or region_data.get("on_demand")
                    or next(
                        (
                            v
                            for v in region_data.values()
                            if isinstance(v, (int, float)) and v > 0
                        ),
                        default,
                    )
                )
            except (TypeError, ValueError):
                return default

        return default

    @staticmethod
    def _flatten_pricing(raw: Dict) -> Dict[str, float]:
        """
        Converts nested pricing dict to flat {region: float}.
        Handles both formats:
          {"us-east-1": 0.096}
          {"us-east-1": {"on_demand_per_vcpu_hr": 0.096}}
          {"us-east-1": {"m5.large": 0.096, ...}}
        """
        result: Dict[str, float] = {}

        for region, value in raw.items():
            if isinstance(value, (int, float)):
                result[region] = float(value)
            elif isinstance(value, dict):
                price = (
                    value.get("on_demand_per_vcpu_hr")
                    or value.get("m5.large")
                    or value.get("on_demand")
                    or next(
                        (
                            v
                            for v in value.values()
                            if isinstance(v, (int, float)) and v > 0
                        ),
                        0.096,
                    )
                )
                result[region] = float(price)

        return result or {
            "us-east-1": 0.096,
            "us-west-2": 0.096,
            "eu-west-1": 0.107,
            "eu-north-1": 0.098,
        }

    def _fetch_from_aws(self) -> Dict:
        """Direct AWS Pricing API call. Used when pipeline file is missing."""
        client = boto3.client("pricing", region_name="us-east-1")
        result: Dict = {}

        paginator = client.get_paginator("get_products")
        pages = paginator.paginate(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            ],
            PaginationConfig={"MaxItems": 1000, "PageSize": 100},
        )

        for page in pages:
            for raw_item in page.get("PriceList", []):
                try:
                    item = json.loads(raw_item)
                    attrs = item["product"]["attributes"]
                    region = _LOCATION_MAP.get(attrs.get("location", ""))
                    inst = attrs.get("instanceType", "")
                    if not region or not inst:
                        continue

                    for term in item.get("terms", {}).get("OnDemand", {}).values():
                        for dim in term.get("priceDimensions", {}).values():
                            price = float(dim.get("pricePerUnit", {}).get("USD", 0))
                            if price > 0:
                                result.setdefault(region, {})[inst] = price
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    continue

        # Ensure regions have generic fields that downstream pricing logic expects.
        for region, region_data in result.items():
            if isinstance(region_data, dict):
                if "on_demand_per_vcpu_hr" not in region_data:
                    fallback_price = (
                        region_data.get("m5.large")
                        or next(
                            (
                                v
                                for v in region_data.values()
                                if isinstance(v, (int, float)) and v > 0
                            ),
                            0.096,
                        )
                    )
                    region_data["on_demand_per_vcpu_hr"] = float(fallback_price)
                region_data.setdefault("spot_discount", 0.65)

        return result

    def _persist_to_file(self, data: Dict) -> None:
        """Saves API-fetched data to file for future use (atomic write)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp.json")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            tmp.replace(self._path)
            try:
                self._file_mtime = self._path.stat().st_mtime
            except OSError:
                pass
        except Exception as exc:
            logger.warning("PricingCache: could not persist to file: %s", exc)
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass