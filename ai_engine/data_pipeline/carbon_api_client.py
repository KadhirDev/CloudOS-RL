"""
Carbon Intensity API Client
============================
Primary source: Electricity Maps v3 API  https://api.electricitymap.org
  - Requires: ELECTRICITY_MAPS_API_KEY in .env
  - Free tier: https://www.electricitymaps.com/free-tier

Fallback chain:
  1. Electricity Maps live /carbon-intensity/latest   (real-time gCO2/kWh)
  2. Electricity Maps /carbon-intensity/history        (recent historical)
  3. In-memory cache of last successful live fetch     (if API temporarily fails)
  4. Hardcoded static empirical values                 (ALWAYS available)

WITHOUT API KEY: returns static values immediately. No network call made.
WITH    API KEY: tries live → history → cache → static per zone.

Output written by DataNormalizer to: data/carbon/carbon_intensity.json
Read by: ai_engine/environment/cloud_env.py
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# httpx import with clear error message
# ---------------------------------------------------------------------------
try:
    import httpx
except ImportError:
    raise ImportError(
        "\n\nhttpx is not installed.\n"
        "Fix: run   pip install httpx   then try again.\n"
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Zone mapping: AWS region slug → Electricity Maps zone code
# ---------------------------------------------------------------------------

AWS_REGION_TO_ZONE: Dict[str, str] = {
    "us-east-1":      "US-MIDA-PJM",
    "us-east-2":      "US-MIDW-MISO",
    "us-west-1":      "US-CAL-CISO",
    "us-west-2":      "US-NW-PACW",
    "eu-west-1":      "IE",
    "eu-west-2":      "GB",
    "eu-west-3":      "FR",
    "eu-central-1":   "DE",
    "eu-north-1":     "SE-SE3",
    "ap-southeast-1": "SG",
    "ap-southeast-2": "AU-NSW",
    "ap-northeast-1": "JP-TK",
    "ap-northeast-2": "KR",
    "ap-south-1":     "IN-WE",
    "ca-central-1":   "CA-ON",
    "sa-east-1":      "BR-CS",
}

# ---------------------------------------------------------------------------
# Static fallback — Ember Climate 2023 annual average data
# Used when API key is absent or API is unreachable
# ---------------------------------------------------------------------------

STATIC_CARBON_INTENSITY: Dict[str, Dict] = {
    "us-east-1":      {"gco2_kwh": 415.0, "renewable_pct": 28.5},
    "us-east-2":      {"gco2_kwh": 432.0, "renewable_pct": 25.1},
    "us-west-1":      {"gco2_kwh": 222.0, "renewable_pct": 52.3},
    "us-west-2":      {"gco2_kwh": 192.0, "renewable_pct": 64.8},
    "eu-west-1":      {"gco2_kwh": 316.0, "renewable_pct": 39.2},
    "eu-west-2":      {"gco2_kwh": 268.0, "renewable_pct": 44.6},
    "eu-west-3":      {"gco2_kwh":  58.0, "renewable_pct": 78.9},
    "eu-central-1":   {"gco2_kwh": 338.0, "renewable_pct": 38.1},
    "eu-north-1":     {"gco2_kwh":  42.0, "renewable_pct": 92.1},
    "ap-southeast-1": {"gco2_kwh": 453.0, "renewable_pct": 18.4},
    "ap-southeast-2": {"gco2_kwh": 618.0, "renewable_pct": 18.7},
    "ap-northeast-1": {"gco2_kwh": 506.0, "renewable_pct": 22.3},
    "ap-northeast-2": {"gco2_kwh": 512.0, "renewable_pct": 19.8},
    "ap-south-1":     {"gco2_kwh": 708.0, "renewable_pct": 11.9},
    "ca-central-1":   {"gco2_kwh":  89.0, "renewable_pct": 76.2},
    "sa-east-1":      {"gco2_kwh": 136.0, "renewable_pct": 62.4},
}

_EM_BASE_URL     = "https://api.electricitymap.org/v3"
_REQUEST_TIMEOUT = 10.0
_BATCH_PAUSE     = 0.1


class CarbonAPIClient:
    """
    Fetches live carbon intensity from Electricity Maps.
    Always returns a non-empty result — worst case is static values.
    """

    def __init__(self, config: Dict):
        self._api_key: Optional[str] = (
            os.getenv("ELECTRICITY_MAPS_API_KEY")
            or config.get("data_pipeline", {}).get("electricity_maps_api_key")
            or ""
        ).strip()

        self._cache: Dict[str, Dict] = {}

    # -----------------------------------------------------------------------
    # Public
    # -----------------------------------------------------------------------

    def fetch(self) -> Dict[str, Dict]:
        """
        Returns carbon intensity for all 16 mapped AWS regions.
        Always returns a complete non-empty dict regardless of API availability.
        """
        try:
            if not self._api_key:
                logger.warning(
                    "ELECTRICITY_MAPS_API_KEY not set. "
                    "Using static carbon values (valid for training). "
                    "For live data: set key in .env — free at https://www.electricitymaps.com/free-tier"
                )
                return self._build_static_all()

            logger.info("CarbonAPIClient: fetching live carbon for %d regions ...", len(AWS_REGION_TO_ZONE))
            t0 = time.perf_counter()

            results: Dict[str, Dict] = {}
            failed_regions: List[str] = []

            for aws_region, zone in AWS_REGION_TO_ZONE.items():
                data, source = self._fetch_zone(zone)
                if data is not None:
                    entry = self._format_live_entry(aws_region, zone, data, source)
                    results[aws_region] = entry
                    self._cache[aws_region] = entry
                else:
                    failed_regions.append(aws_region)
                time.sleep(_BATCH_PAUSE)

            for region in failed_regions:
                if region in self._cache:
                    results[region] = {**self._cache[region], "data_source": "cached_live"}
                else:
                    results[region] = self._static_entry(region)

            live = sum(1 for v in results.values() if "live" in v.get("data_source", ""))
            cached = sum(1 for v in results.values() if "cached" in v.get("data_source", ""))
            static = sum(1 for v in results.values() if "static" in v.get("data_source", ""))
            elapsed = (time.perf_counter() - t0) * 1000

            logger.info(
                "CarbonAPIClient: %d regions — live=%d cached=%d static=%d (%.0f ms)",
                len(results), live, cached, static, elapsed,
            )
            return results

        except Exception as exc:
            logger.warning(
                "CarbonAPIClient.fetch failed (%s) — using static fallback", exc
            )
            return self._static_fallback()

    # -----------------------------------------------------------------------
    # Private: API calls
    # -----------------------------------------------------------------------

    def _fetch_zone(self, zone: str) -> Tuple[Optional[Dict], str]:
        headers = {
            "auth-token":   self._api_key,
            "Content-Type": "application/json",
        }

        endpoints = [
            (f"{_EM_BASE_URL}/carbon-intensity/latest?zone={zone}",  "electricity_maps_live"),
            (f"{_EM_BASE_URL}/carbon-intensity/history?zone={zone}",  "electricity_maps_history"),
        ]

        for url, label in endpoints:
            try:
                with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
                    resp = client.get(url, headers=headers)

                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and "history" in data:
                        history = data["history"]
                        if history:
                            data = history[-1]
                    return data, label

                elif resp.status_code == 401:
                    logger.error(
                        "CarbonAPIClient: Invalid API key (401). "
                        "Check ELECTRICITY_MAPS_API_KEY in your .env file."
                    )
                    return None, "invalid_api_key"

                elif resp.status_code == 404:
                    logger.debug("Zone %s: not found (404).", zone)
                    return None, "zone_not_found"

                elif resp.status_code == 429:
                    logger.warning("CarbonAPIClient: Rate limit (429). Pausing 60s ...")
                    time.sleep(60.0)

            except httpx.TimeoutException:
                logger.warning("CarbonAPIClient: timeout — zone %s", zone)
            except httpx.RequestError as exc:
                logger.warning("CarbonAPIClient: request error — zone %s: %s", zone, exc)

        return None, "api_unavailable"

    # -----------------------------------------------------------------------
    # Private: formatting
    # -----------------------------------------------------------------------

    @staticmethod
    def _format_live_entry(aws_region: str, zone: str, data: Dict, source: str) -> Dict:
        carbon = float(data.get("carbonIntensity", 0.0))

        power  = data.get("powerProductionBreakdown", {})
        total  = sum(v for v in power.values() if isinstance(v, (int, float)) and v > 0)
        renew  = sum(
            v for k, v in power.items()
            if isinstance(v, (int, float)) and v > 0
            and k in ("wind", "solar", "hydro", "geothermal", "biomass", "nuclear")
        )
        renewable_pct = (
            renew / total * 100.0 if total > 0
            else STATIC_CARBON_INTENSITY.get(aws_region, {}).get("renewable_pct", 30.0)
        )

        return {
            "zone":                          zone,
            "carbon_intensity_gco2_per_kwh": round(carbon, 2),
            "gco2_per_kwh":                  round(carbon, 2),
            "renewable_pct":                 round(renewable_pct, 1),
            "data_source":                   source,
            "fetched_at":                    datetime.now(tz=timezone.utc).isoformat(),
            "raw_datetime":                  data.get("datetime", ""),
        }

    @staticmethod
    def _static_entry(aws_region: str) -> Dict:
        s    = STATIC_CARBON_INTENSITY.get(aws_region, {"gco2_kwh": 400.0, "renewable_pct": 30.0})
        zone = AWS_REGION_TO_ZONE.get(aws_region, "unknown")
        return {
            "zone":                          zone,
            "carbon_intensity_gco2_per_kwh": s["gco2_kwh"],
            "gco2_per_kwh":                  s["gco2_kwh"],
            "renewable_pct":                 s["renewable_pct"],
            "data_source":                   "static_fallback",
            "fetched_at":                    datetime.now(tz=timezone.utc).isoformat(),
            "raw_datetime":                  "",
        }

    def _build_static_all(self) -> Dict[str, Dict]:
        return {region: self._static_entry(region) for region in AWS_REGION_TO_ZONE}
    def _static_fallback(self) -> dict:
        """
        Returns static carbon intensity fallback data for 16 AWS regions.
        Used when Electricity Maps API key is absent or call fails.
        eu-north-1 is cleanest at 42 gCO2/kWh.
        """
        return {
            "us-east-1": {"gco2_per_kwh": 415.0, "source": "static"},
            "us-east-2": {"gco2_per_kwh": 410.0, "source": "static"},
            "us-west-1": {"gco2_per_kwh": 252.0, "source": "static"},
            "us-west-2": {"gco2_per_kwh": 192.0, "source": "static"},
            "eu-west-1": {"gco2_per_kwh": 316.0, "source": "static"},
            "eu-west-2": {"gco2_per_kwh": 225.0, "source": "static"},
            "eu-west-3": {"gco2_per_kwh": 58.0, "source": "static"},
            "eu-central-1": {"gco2_per_kwh": 338.0, "source": "static"},
            "eu-north-1": {"gco2_per_kwh": 42.0, "source": "static"},
            "ap-southeast-1": {"gco2_per_kwh": 453.0, "source": "static"},
            "ap-southeast-2": {"gco2_per_kwh": 610.0, "source": "static"},
            "ap-northeast-1": {"gco2_per_kwh": 506.0, "source": "static"},
            "ap-northeast-2": {"gco2_per_kwh": 415.0, "source": "static"},
            "ap-south-1": {"gco2_per_kwh": 708.0, "source": "static"},
            "sa-east-1": {"gco2_per_kwh": 136.0, "source": "static"},
            "ca-central-1": {"gco2_per_kwh": 89.0, "source": "static"},
        }