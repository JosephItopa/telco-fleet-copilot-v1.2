"""Scan orchestration: discover pods, attach metrics, emit wire records."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Optional, Protocol, Sequence

from .collectors.prometheus import cluster_apps
from .config import ClusterConfig
from .metrics_schema import DEFAULT_METRICS, MetricDefinition
from .models import AppMetric, AppRef
from .timeutil import to_iso8601, utcnow

logger = logging.getLogger(__name__)


class AppCollector(Protocol):
    async def collect_apps(self) -> list[AppRef]: ...


class MetricsCollector(Protocol):
    async def collect(self, apps_by_cluster, clusters) -> Mapping[str, object]: ...


@dataclass(slots=True)
class ScanResult:
    started_at: datetime
    finished_at: datetime
    metrics: list[AppMetric] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def cluster_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for metric in self.metrics:
            key = metric.cluster or "unknown"
            counts[key] = counts.get(key, 0) + 1
        return counts


class FleetScanner:
    """Runs one full fleet scan and returns the records to publish."""

    def __init__(
        self,
        kubernetes: AppCollector,
        prometheus: MetricsCollector,
        clusters: Sequence[ClusterConfig],
        *,
        schema: Sequence[MetricDefinition] = DEFAULT_METRICS,
        clock=utcnow,
    ) -> None:
        self._kubernetes = kubernetes
        self._prometheus = prometheus
        self._clusters = {cluster.name: cluster for cluster in clusters}
        self._schema = tuple(schema)
        self._clock = clock

    async def scan_once(self) -> ScanResult:
        started_at = self._clock()
        errors: list[str] = []

        apps, discovery_errors = await self._discover()
        errors.extend(discovery_errors)

        indexes: Mapping[str, object] = {}
        if apps:
            grouped = cluster_apps(apps)
            try:
                indexes = await self._prometheus.collect(grouped, self._clusters)
            except Exception as exc:
                message = f"metrics collection failed: {exc}"
                logger.error(message)
                errors.append(message)

        timestamp = to_iso8601(started_at)
        metrics = [
            self._build_metric(app, indexes.get(app.cluster), timestamp) for app in apps
        ]
        metrics.sort(key=lambda metric: metric.kafka_key)
        finished_at = self._clock()

        logger.info(
            "scan finished in %.2fs: %d applications across %d clusters",
            (finished_at - started_at).total_seconds(),
            len(metrics),
            len({metric.cluster for metric in metrics}),
        )
        return ScanResult(
            started_at=started_at,
            finished_at=finished_at,
            metrics=metrics,
            errors=errors,
        )

    async def _discover(self) -> tuple[list[AppRef], list[str]]:
        try:
            return await self._kubernetes.collect_apps(), []
        except Exception as exc:
            logger.error("pod discovery failed: %s", exc)
            return [], [f"pod discovery failed: {exc}"]

    def _build_metric(
        self,
        app: AppRef,
        index: Optional[object],
        timestamp: str,
    ) -> AppMetric:
        values: dict[str, float] = {}
        if index is not None:
            for definition in self._schema:
                value = index.resolve(definition.field, definition.scope, app)  # type: ignore[attr-defined]
                if value is not None:
                    values[definition.field] = value
        return AppMetric.from_ref(app, timestamp=timestamp, values=values)
