"""Shared test factories."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Mapping, Optional, Sequence

from fleet_copilot.config import AppConfig, ClusterConfig, KafkaConfig, PrometheusConfig, ScanConfig
from fleet_copilot.models import AppRef

NOW = datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone.utc)


def make_cluster(name: str = "prod-eu", url: Optional[str] = None, verify_ssl: bool = True):
    return ClusterConfig(name=name, prometheus_url=url, verify_ssl=verify_ssl)


def make_config(
    *,
    interval: float = 180.0,
    clusters: Sequence[ClusterConfig] = (),
    mock: bool = False,
) -> AppConfig:
    return AppConfig(
        kafka=KafkaConfig(bootstrap_servers="localhost:9092"),
        scan=ScanConfig(interval_seconds=interval),
        prometheus=PrometheusConfig(),
        clusters=tuple(clusters),
        mock_mode=mock,
    )


def make_app_ref(
    *,
    cluster: str = "prod-eu",
    namespace: str = "production",
    service: str = "payment-api",
    pod: str = "payment-api-7d9f8",
    restart_count: int = 3,
    pod_ready: bool = True,
    liveness: bool = True,
    readiness: bool = True,
    labels: Optional[Mapping[str, str]] = None,
) -> AppRef:
    return AppRef(
        cluster=cluster,
        namespace=namespace,
        service=service,
        pod=pod,
        timestamp=NOW,
        restart_count=restart_count,
        pod_ready=pod_ready,
        liveness=liveness,
        readiness=readiness,
        labels=dict(labels or {}),
    )


def make_pod(
    *,
    name: str = "payment-api-7d9f8",
    namespace: str = "production",
    labels: Mapping[str, str] | None = None,
    owner: bool = True,
    phase: str = "Running",
    ready: bool = True,
    restart_count: int = 3,
    waiting_reason: Optional[str] = None,
    deletion_timestamp: Any = None,
    container_name: str = "payment-api",
) -> SimpleNamespace:
    waiting = SimpleNamespace(reason=waiting_reason) if waiting_reason else None
    container_status = SimpleNamespace(
        restart_count=restart_count,
        ready=ready,
        state=SimpleNamespace(waiting=waiting, terminated=None),
    )
    status = SimpleNamespace(
        phase=phase,
        container_statuses=[container_status],
        conditions=[SimpleNamespace(type="Ready", status="True" if ready else "False")],
    )
    metadata = SimpleNamespace(
        name=name,
        namespace=namespace,
        labels=dict(labels if labels is not None else {"app": "payment-api"}),
        deletion_timestamp=deletion_timestamp,
        owner_references=[SimpleNamespace(kind="ReplicaSet")] if owner else [],
    )
    spec = SimpleNamespace(containers=[SimpleNamespace(name=container_name)])
    return SimpleNamespace(metadata=metadata, status=status, spec=spec)


class FakeResponse:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status = status

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, content_type: Optional[str] = None) -> Any:
        return self._payload

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return False


class FakeSession:
    """Minimal aiohttp.ClientSession stand-in returning a canned payload."""

    def __init__(self, payload: Any) -> None:
        self._payload = payload
        self.queries: list[str] = []

    def get(self, url: str, params: Optional[dict] = None, ssl: Any = None) -> FakeResponse:
        self.queries.append((params or {}).get("query", ""))
        return FakeResponse(self._payload)


def vector_payload(series: list[tuple[dict, str]]) -> dict:
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": labels, "value": [1758110400.0, value]} for labels, value in series
            ],
        },
    }
