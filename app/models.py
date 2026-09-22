from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class WorkloadTarget:
    app_id: str
    namespace: str
    pod_name: str
    pod_ip: str | None
    scrape_url: str | None
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class MetricsEvent:
    app_id: str
    namespace: str
    pod: str
    cluster: str
    timestamp: str
    sources: list[str]
    metrics: dict[str, Any]
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def now(
        cls,
        app_id: str,
        namespace: str,
        pod: str,
        cluster: str,
        sources: list[str],
        metrics: dict[str, Any],
        labels: dict[str, str],
    ) -> "MetricsEvent":
        return cls(
            app_id=app_id,
            namespace=namespace,
            pod=pod,
            cluster=cluster,
            timestamp=datetime.now(timezone.utc).isoformat(),
            sources=sources,
            metrics=metrics,
            labels=labels,
        )

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__
