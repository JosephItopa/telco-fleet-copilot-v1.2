"""Deterministic in-memory providers for local runs, demos and tests.

Enable with ``MOCK_MODE=true``. Values are derived from a SHA-256 of the pod
key, so every run produces the same stream without a cluster or Prometheus.
"""

from __future__ import annotations

import hashlib
import random
from typing import Mapping, Sequence

from .collectors.prometheus import MetricsIndex
from .config import ClusterConfig
from .metrics_schema import DEFAULT_METRICS
from .models import AppRef
from .timeutil import utcnow

MOCK_CLUSTERS: tuple[ClusterConfig, ...] = (
    ClusterConfig(name="mock-eu"),
    ClusterConfig(name="mock-us"),
)
MOCK_NAMESPACES = ("production", "payments", "core", "staging")
MOCK_SERVICES = (
    "payment-api",
    "auth-service",
    "billing-worker",
    "notification-svc",
    "customer-api",
    "orders-api",
    "inventory-svc",
    "search-api",
    "ledger-worker",
    "gateway-edge",
    "profile-api",
    "reporting-svc",
)


def _seed(key: str) -> random.Random:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


class MockKubernetesCollector:
    """Synthesises a fleet of application pods."""

    def __init__(
        self,
        apps: int = 1200,
        clusters: Sequence[ClusterConfig] = MOCK_CLUSTERS,
    ) -> None:
        self._apps = apps
        self._clusters = tuple(clusters)

    async def collect_apps(self) -> list[AppRef]:
        now = utcnow()
        pods: list[AppRef] = []
        per_cluster = max(1, self._apps // max(1, len(self._clusters)))
        for cluster in self._clusters:
            for index in range(per_cluster):
                service = MOCK_SERVICES[index % len(MOCK_SERVICES)]
                namespace = MOCK_NAMESPACES[index % len(MOCK_NAMESPACES)]
                pod = f"{service}-{_suffix(cluster.name, namespace, service, index)}"
                key = f"{cluster.name}/{namespace}/{service}/{pod}"
                rng = _seed(key)
                replicas_restarted = rng.random() < 0.08
                pods.append(
                    AppRef(
                        cluster=cluster.name,
                        namespace=namespace,
                        service=service,
                        pod=pod,
                        timestamp=now,
                        restart_count=rng.randint(1, 7) if replicas_restarted else 0,
                        pod_ready=rng.random() > 0.05,
                        liveness=rng.random() > 0.03,
                        readiness=rng.random() > 0.07,
                        labels={"app": service},
                    )
                )
        return pods


class MockPrometheusCollector:
    """Synthesises metrics for the mock fleet, including a few hot pods."""

    def __init__(self, problem_every: int = 37) -> None:
        self._problem_every = max(1, problem_every)

    async def collect(
        self,
        apps_by_cluster: Mapping[str, Sequence[AppRef]],
        clusters: Mapping[str, ClusterConfig],
    ) -> dict[str, MetricsIndex]:
        indexes: dict[str, MetricsIndex] = {}
        counter = 0
        for cluster, apps in apps_by_cluster.items():
            index = MetricsIndex()
            for app in apps:
                counter += 1
                rng = _seed(f"{app.key}:metrics")
                hot = counter % self._problem_every == 0
                values = _values(rng, hot)
                for definition in DEFAULT_METRICS:
                    value = values[definition.field]
                    if definition.scope == "pod":
                        index.add_pod(definition.field, app.namespace, app.pod, value)
                    else:
                        index.add_service(definition.field, app.namespace, app.service, value)
            indexes[cluster] = index
        return indexes


def _values(rng: random.Random, hot: bool) -> dict[str, float]:
    if hot:
        return {
            "cpu_usage_percent": rng.uniform(84.0, 99.0),
            "memory_usage_percent": rng.uniform(88.0, 98.0),
            "cpu_throttling_percent": rng.uniform(12.0, 45.0),
            "request_rate": rng.uniform(180.0, 420.0),
            "error_rate_percent": rng.uniform(5.0, 18.0),
            "p95_latency_ms": rng.uniform(1000.0, 3200.0),
            "active_requests": float(rng.randint(40, 90)),
            "disk_usage_percent": rng.uniform(72.0, 93.0),
            "db_latency_ms": rng.uniform(320.0, 900.0),
            "db_connection_pool_percent": rng.uniform(80.0, 97.0),
        }
    return {
        "cpu_usage_percent": rng.uniform(2.0, 72.0),
        "memory_usage_percent": rng.uniform(15.0, 78.0),
        "cpu_throttling_percent": rng.uniform(0.0, 9.0),
        "request_rate": rng.uniform(5.0, 260.0),
        "error_rate_percent": rng.uniform(0.0, 3.5),
        "p95_latency_ms": rng.uniform(25.0, 850.0),
        "active_requests": float(rng.randint(0, 60)),
        "disk_usage_percent": rng.uniform(8.0, 70.0),
        "db_latency_ms": rng.uniform(5.0, 220.0),
        "db_connection_pool_percent": rng.uniform(5.0, 75.0),
    }


def _suffix(*parts: object) -> str:
    digest = hashlib.sha1("|".join(str(part) for part in parts).encode()).hexdigest()
    return digest[:8]
