"""Wire models.

``AppMetric`` is the exact Kafka payload contract. Field order follows the
agreed sample so the serialized JSON is stable and easy to eyeball. ``cluster``
is appended as an additive field because the scanner covers several clusters;
consumers that ignore unknown fields are unaffected.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional

from .timeutil import to_iso8601

# Label keys, in priority order, that identify the application behind a pod.
SERVICE_LABEL_KEYS: tuple[str, ...] = (
    "app.kubernetes.io/name",
    "app.kubernetes.io/instance",
    "app",
    "k8s-app",
    "service",
)

# Prometheus label keys that may carry the application name on a series.
PROMETHEUS_SERVICE_LABELS: tuple[str, ...] = (
    "service",
    "app",
    "app_kubernetes_io_name",
    "k8s_app",
    "job",
)


@dataclass(slots=True)
class AppRef:
    """A single pod discovered on a cluster, before metrics are attached."""

    cluster: str
    namespace: str
    service: str
    pod: str
    timestamp: datetime
    restart_count: int = 0
    pod_ready: bool = False
    liveness: bool = False
    readiness: bool = False
    labels: Mapping[str, str] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.cluster}/{self.namespace}/{self.service}/{self.pod}"


@dataclass(slots=True)
class AppMetric:
    """One application observation published to Kafka."""

    timestamp: str
    service: str
    namespace: str
    pod: str
    cpu_usage_percent: Optional[float]
    memory_usage_percent: Optional[float]
    cpu_throttling_percent: Optional[float]
    request_rate: Optional[float]
    error_rate_percent: Optional[float]
    p95_latency_ms: Optional[float]
    active_requests: Optional[int]
    restart_count: int
    pod_ready: bool
    disk_usage_percent: Optional[float]
    db_latency_ms: Optional[float]
    db_connection_pool_percent: Optional[float]
    liveness: bool
    readiness: bool
    cluster: Optional[str] = None

    @classmethod
    def from_ref(
        cls,
        ref: AppRef,
        *,
        timestamp: Optional[str] = None,
        values: Optional[Mapping[str, Any]] = None,
    ) -> "AppMetric":
        values = values or {}
        return cls(
            timestamp=timestamp or to_iso8601(ref.timestamp),
            service=ref.service,
            namespace=ref.namespace,
            pod=ref.pod,
            cpu_usage_percent=_as_float(values.get("cpu_usage_percent")),
            memory_usage_percent=_as_float(values.get("memory_usage_percent")),
            cpu_throttling_percent=_as_float(values.get("cpu_throttling_percent")),
            request_rate=_as_float(values.get("request_rate")),
            error_rate_percent=_as_float(values.get("error_rate_percent")),
            p95_latency_ms=_as_float(values.get("p95_latency_ms")),
            active_requests=_as_int(values.get("active_requests")),
            restart_count=ref.restart_count,
            pod_ready=ref.pod_ready,
            disk_usage_percent=_as_float(values.get("disk_usage_percent")),
            db_latency_ms=_as_float(values.get("db_latency_ms")),
            db_connection_pool_percent=_as_float(values.get("db_connection_pool_percent")),
            liveness=ref.liveness,
            readiness=ref.readiness,
            cluster=ref.cluster,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "namespace": self.namespace,
            "pod": self.pod,
            "cpu_usage_percent": self.cpu_usage_percent,
            "memory_usage_percent": self.memory_usage_percent,
            "cpu_throttling_percent": self.cpu_throttling_percent,
            "request_rate": self.request_rate,
            "error_rate_percent": self.error_rate_percent,
            "p95_latency_ms": self.p95_latency_ms,
            "active_requests": self.active_requests,
            "restart_count": self.restart_count,
            "pod_ready": self.pod_ready,
            "disk_usage_percent": self.disk_usage_percent,
            "db_latency_ms": self.db_latency_ms,
            "db_connection_pool_percent": self.db_connection_pool_percent,
            "liveness": self.liveness,
            "readiness": self.readiness,
            "cluster": self.cluster,
        }

    @property
    def kafka_key(self) -> str:
        """Partition key: keeps every pod's observations on one partition."""
        return f"{self.cluster}/{self.namespace}/{self.service}/{self.pod}"


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return round(number, 4)


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None
