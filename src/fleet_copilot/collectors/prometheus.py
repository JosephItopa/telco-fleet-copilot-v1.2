"""Prometheus collection.

One instant query per metric definition returns a vector for *all* applications
in a cluster. Results are indexed in memory by pod and by application label, so
attaching metrics to thousands of pods costs no extra round-trips.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Iterable, Mapping, Optional, Sequence

from ..config import ClusterConfig
from ..errors import CollectionError
from ..metrics_schema import SCOPE_POD, SCOPE_SERVICE, MetricDefinition
from ..models import PROMETHEUS_SERVICE_LABELS, AppRef

logger = logging.getLogger(__name__)


class MetricsIndex:
    """Lookup table of Prometheus values for one cluster."""

    __slots__ = ("_by_pod", "_by_service")

    def __init__(self) -> None:
        self._by_pod: dict[str, dict[tuple[str, str], float]] = {}
        self._by_service: dict[str, dict[tuple[str, str], float]] = {}

    def add_series(self, field: str, labels: Mapping[str, Any], value: float) -> None:
        namespace = _label(labels, "namespace") or ""
        pod = _label(labels, "pod")
        if pod:
            self._by_pod.setdefault(field, {})[(namespace, pod)] = value
        for key in PROMETHEUS_SERVICE_LABELS:
            name = _label(labels, key)
            if name:
                self._by_service.setdefault(field, {})[(namespace, name)] = value
                break

    def add_pod(self, field: str, namespace: str, pod: str, value: float) -> None:
        self._by_pod.setdefault(field, {})[(namespace, pod)] = value

    def add_service(self, field: str, namespace: str, name: str, value: float) -> None:
        self._by_service.setdefault(field, {})[(namespace, name)] = value

    def get_pod(self, field: str, namespace: str, pod: str) -> Optional[float]:
        return self._by_pod.get(field, {}).get((namespace, pod))

    def get_service(self, field: str, namespace: str, name: str) -> Optional[float]:
        return self._by_service.get(field, {}).get((namespace, name))

    def resolve(
        self,
        field: str,
        scope: str,
        app: AppRef,
    ) -> Optional[float]:
        """Resolve a field for an app, honouring scope with a sensible fallback."""
        if scope == SCOPE_POD:
            value = self.get_pod(field, app.namespace, app.pod)
            if value is not None:
                return value
        else:
            value = self.get_service(field, app.namespace, app.service)
            if value is not None:
                return value
            for name in app.labels.values():
                value = self.get_service(field, app.namespace, name)
                if value is not None:
                    return value
        return self.get_pod(field, app.namespace, app.pod)

    def __len__(self) -> int:
        return sum(len(values) for values in self._by_service.values()) + sum(
            len(values) for values in self._by_pod.values()
        )


class PrometheusCollector:
    def __init__(
        self,
        schema: Sequence[MetricDefinition],
        *,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._schema = tuple(schema)
        self._timeout = timeout_seconds

    async def collect(
        self,
        apps_by_cluster: Mapping[str, Sequence[AppRef]],
        clusters: Mapping[str, ClusterConfig],
    ) -> dict[str, MetricsIndex]:
        targets = [
            (name, clusters[name])
            for name, apps in apps_by_cluster.items()
            if apps and name in clusters and clusters[name].prometheus_url
        ]
        if not targets:
            return {name: MetricsIndex() for name in apps_by_cluster}

        try:
            import aiohttp
        except ImportError as exc:  # pragma: no cover - dependency present in prod
            raise CollectionError(
                "the 'aiohttp' package is required to query Prometheus; pip install aiohttp"
            ) from exc

        timeout = aiohttp.ClientTimeout(total=self._timeout)
        indexes: dict[str, MetricsIndex] = {}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            results = await asyncio.gather(
                *(
                    self._collect_cluster(session, name, cluster)
                    for name, cluster in targets
                ),
                return_exceptions=True,
            )
        for (name, _cluster), result in zip(targets, results):
            if isinstance(result, BaseException):
                logger.error("prometheus scan for cluster %s failed: %s", name, result)
                indexes[name] = MetricsIndex()
            else:
                indexes[name] = result
        for name in apps_by_cluster:
            indexes.setdefault(name, MetricsIndex())
        return indexes

    async def _collect_cluster(self, session, name: str, cluster: ClusterConfig) -> MetricsIndex:
        index = MetricsIndex()
        assert cluster.prometheus_url is not None
        ssl_arg: Any = None if cluster.verify_ssl else False
        responses = await asyncio.gather(
            *(
                self._query(session, cluster.prometheus_url, definition, ssl_arg)
                for definition in self._schema
            ),
            return_exceptions=True,
        )
        for definition, response in zip(self._schema, responses):
            if isinstance(response, BaseException):
                logger.warning(
                    "prometheus query for %s on %s failed: %s",
                    definition.field,
                    name,
                    response,
                )
                continue
            for labels, value in response:
                index.add_series(definition.field, labels, value)
        logger.info("prometheus cluster %s: indexed %d series", name, len(index))
        return index

    async def _query(
        self,
        session,
        base_url: str,
        definition: MetricDefinition,
        ssl_arg: Any,
    ) -> list[tuple[Mapping[str, Any], float]]:
        endpoint = base_url.rstrip("/") + "/api/v1/query"
        try:
            async with session.get(
                endpoint,
                params={"query": definition.query},
                ssl=ssl_arg,
            ) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)
        except Exception as exc:
            raise CollectionError(f"query {definition.field!r} failed: {exc}") from exc
        return self.parse_response(payload, definition.field)

    @staticmethod
    def parse_response(payload: Any, field: str = "") -> list[tuple[Mapping[str, Any], float]]:
        if not isinstance(payload, Mapping) or payload.get("status") != "success":
            raise CollectionError(
                f"prometheus returned status {payload.get('status')!r} for {field or 'query'}"
            )
        data = payload.get("data") or {}
        results = data.get("result") or []
        parsed: list[tuple[Mapping[str, Any], float]] = []
        for series in results:
            value = _parse_sample(series.get("value"))
            if value is None:
                continue
            parsed.append((series.get("metric") or {}, value))
        return parsed


def _parse_sample(sample: Any) -> Optional[float]:
    if not sample or not isinstance(sample, (list, tuple)) or len(sample) < 2:
        return None
    raw = sample[1]
    if isinstance(raw, Mapping):  # native histogram buckets, not supported
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value != value:  # NaN
        return None
    return value


def _label(labels: Mapping[str, Any], key: str) -> Optional[str]:
    value = labels.get(key)
    if value is None:
        return None
    value = str(value)
    return value or None


def cluster_apps(apps: Iterable[AppRef]) -> dict[str, list[AppRef]]:
    grouped: dict[str, list[AppRef]] = {}
    for app in apps:
        grouped.setdefault(app.cluster, []).append(app)
    return grouped


__all__ = ["MetricsIndex", "PrometheusCollector", "cluster_apps", "SCOPE_POD", "SCOPE_SERVICE"]
