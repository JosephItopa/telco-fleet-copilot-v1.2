import logging
from typing import Any
from kubernetes import client

LOG = logging.getLogger(__name__)


class KubernetesMetricsAPI:
    """Reads CPU and memory usage from metrics.k8s.io."""

    def __init__(self):
        self.api = client.CustomObjectsApi()

    def get_pod_metrics(self, namespace: str, pod_name: str) -> dict[str, Any]:
        try:
            obj = self.api.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=namespace,
                plural="pods",
                name=pod_name,
            )
        except Exception as exc:
            LOG.warning(
                "Metrics API failed for %s/%s: %s",
                namespace,
                pod_name,
                exc,
            )
            return {}

        cpu_nano = 0
        memory_bytes = 0

        for container in obj.get("containers", []):
            usage = container.get("usage", {})
            cpu_nano += self._parse_cpu(usage.get("cpu", "0"))
            memory_bytes += self._parse_memory(usage.get("memory", "0"))

        return {
            "cpu_usage_cores": cpu_nano / 1_000_000_000,
            "memory_usage_bytes": memory_bytes,
        }

    @staticmethod
    def _parse_cpu(value: str) -> int:
        if value.endswith("n"):
            return int(value[:-1])
        if value.endswith("u"):
            return int(float(value[:-1]) * 1_000)
        if value.endswith("m"):
            return int(float(value[:-1]) * 1_000_000)
        return int(float(value) * 1_000_000_000)

    @staticmethod
    def _parse_memory(value: str) -> int:
        units = {
            "Ki": 1024,
            "Mi": 1024**2,
            "Gi": 1024**3,
            "Ti": 1024**4,
            "K": 1000,
            "M": 1000**2,
            "G": 1000**3,
            "T": 1000**4,
        }
        for suffix, multiplier in units.items():
            if value.endswith(suffix):
                return int(float(value[:-len(suffix)]) * multiplier)
        return int(float(value))
