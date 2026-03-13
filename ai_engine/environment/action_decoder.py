from typing import Dict
import numpy as np


class ActionDecoder:
    CLOUDS = ["aws", "gcp", "azure", "hybrid"]
    REGIONS = [
        "us-east-1", "us-west-2", "eu-west-1", "eu-central-1",
        "ap-southeast-1", "ap-northeast-1", "us-central1",
        "europe-west4", "eastus", "westeurope",
    ]
    INSTANCE_TYPES = [
        "t3.medium", "t3.large", "m5.large", "m5.xlarge",
        "c5.large", "c5.xlarge", "r5.large", "r5.xlarge",
        "g4dn.xlarge", "p3.2xlarge",
    ]
    SCALING_LEVELS = [1, 2, 4, 8]
    PURCHASE_OPTIONS = [
        "on_demand", "spot", "reserved_1yr",
        "reserved_3yr", "savings_plan", "preemptible",
    ]
    SLA_TIERS = [1, 2, 3, 4, 5, 6]

    _CLOUD_REGIONS: Dict[str, list] = {
        "aws":   ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-northeast-1"],
        "gcp":   ["us-central1", "us-east1", "europe-west1", "europe-west4", "asia-southeast1", "asia-northeast1"],
        "azure": ["eastus", "westus2", "northeurope", "westeurope", "southeastasia", "japaneast"],
        "oci":   ["us-ashburn-1", "us-phoenix-1", "eu-frankfurt-1", "eu-amsterdam-1", "ap-singapore-1", "ap-tokyo-1"],
    }

    # Maps generic region indices to provider-specific equivalents
    _REGION_IDX_MAP: Dict[str, int] = {
        "us-east-1": 0, "us-west-2": 1, "eu-west-1": 2, "eu-central-1": 3,
        "ap-southeast-1": 4, "ap-northeast-1": 5, "us-central1": 0,
        "europe-west4": 3, "eastus": 0, "westeurope": 3,
    }

    def decode(self, action: np.ndarray) -> Dict:
        c_idx  = int(action[0]) % len(self.CLOUDS)
        r_idx  = int(action[1]) % len(self.REGIONS)
        i_idx  = int(action[2]) % len(self.INSTANCE_TYPES)
        s_idx  = int(action[3]) % len(self.SCALING_LEVELS)
        p_idx  = int(action[4]) % len(self.PURCHASE_OPTIONS)
        sl_idx = int(action[5]) % len(self.SLA_TIERS)

        cloud          = self.CLOUDS[c_idx]
        generic_region = self.REGIONS[r_idx]
        instance_type  = self.INSTANCE_TYPES[i_idx]
        scaling_level  = self.SCALING_LEVELS[s_idx]
        purchase_opt   = self.PURCHASE_OPTIONS[p_idx]
        sla_tier       = self.SLA_TIERS[sl_idx]

        provider_region = self._map_region(cloud, generic_region)

        return {
            "cloud":            cloud,
            "region":           provider_region,
            "generic_region":   generic_region,
            "instance_type":    instance_type,
            "scaling_level":    scaling_level,
            "purchase_option":  purchase_opt,
            "sla_tier":         sla_tier,
            "requires_migration": cloud != "aws",
        }

    def _map_region(self, cloud: str, generic_region: str) -> str:
        available = self._CLOUD_REGIONS.get(cloud, [generic_region])
        idx = self._REGION_IDX_MAP.get(generic_region, 0) % len(available)
        return available[idx]
