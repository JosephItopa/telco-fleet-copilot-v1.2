"""Prometheus metric definitions and defaults.

Metric names are organisation specific, so every PromQL query is overridable
with a JSON file (``PROMETHEUS_METRIC_QUERIES_FILE``). The file is a list of
objects::

    [
      {"field": "request_rate", "scope": "service",
       "query": "sum by (namespace, service) (rate(my_http_total[5m]))"}
    ]

A definition is fetched with a single instant query per metric, and results are
indexed by ``(namespace, pod)`` and ``(namespace, <app label>)``. That keeps the
number of Prometheus round-trips constant (~one per metric) regardless of
whether there are 10 or 5000 applications.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .errors import ConfigError

SCOPE_POD = "pod"
SCOPE_SERVICE = "service"
VALID_SCOPES = frozenset({SCOPE_POD, SCOPE_SERVICE})

# Fields that are always taken from the Kubernetes API, never from Prometheus.
KUBERNETES_FIELDS = frozenset({"restart_count", "pod_ready", "liveness", "readiness"})

NUMERIC_FIELDS = (
    "cpu_usage_percent",
    "memory_usage_percent",
    "cpu_throttling_percent",
    "request_rate",
    "error_rate_percent",
    "p95_latency_ms",
    "active_requests",
    "disk_usage_percent",
    "db_latency_ms",
    "db_connection_pool_percent",
)


@dataclass(frozen=True)
class MetricDefinition:
    field: str
    query: str
    scope: str = SCOPE_SERVICE


DEFAULT_METRICS: tuple[MetricDefinition, ...] = (
    MetricDefinition(
        field="cpu_usage_percent",
        scope=SCOPE_POD,
        query=(
            "100 * sum by (namespace, pod) "
            '(rate(container_cpu_usage_seconds_total{container!="",container!="POD",image!=""}[5m]))'
            " / clamp_min("
            "sum by (namespace, pod) "
            '(kube_pod_container_resource_limits{resource="cpu",container!=""}), 0.001)'
        ),
    ),
    MetricDefinition(
        field="memory_usage_percent",
        scope=SCOPE_POD,
        query=(
            "100 * sum by (namespace, pod) "
            '(container_memory_working_set_bytes{container!="",container!="POD",image!=""})'
            " / clamp_min("
            "sum by (namespace, pod) "
            '(kube_pod_container_resource_limits{resource="memory",container!=""}), 1)'
        ),
    ),
    MetricDefinition(
        field="cpu_throttling_percent",
        scope=SCOPE_POD,
        query=(
            "100 * sum by (namespace, pod) "
            '(rate(container_cpu_cfs_throttled_periods_total{container!=""}[5m]))'
            " / clamp_min("
            "sum by (namespace, pod) "
            '(rate(container_cpu_cfs_periods_total{container!=""}[5m])), 0.001)'
        ),
    ),
    MetricDefinition(
        field="request_rate",
        scope=SCOPE_SERVICE,
        query="sum by (namespace, service) (rate(http_requests_total[5m]))",
    ),
    MetricDefinition(
        field="error_rate_percent",
        scope=SCOPE_SERVICE,
        query=(
            "100 * sum by (namespace, service) "
            '(rate(http_requests_total{status=~"5.."}[5m]))'
            " / clamp_min("
            "sum by (namespace, service) (rate(http_requests_total[5m])), 0.001)"
        ),
    ),
    MetricDefinition(
        field="p95_latency_ms",
        scope=SCOPE_SERVICE,
        query=(
            "1000 * histogram_quantile(0.95, "
            "sum by (namespace, service, le) "
            "(rate(http_request_duration_seconds_bucket[5m])))"
        ),
    ),
    MetricDefinition(
        field="active_requests",
        scope=SCOPE_SERVICE,
        query=(
            "sum by (namespace, service) (http_requests_in_flight)"
            " or sum by (namespace, service) (http_requests_active)"
        ),
    ),
    MetricDefinition(
        field="disk_usage_percent",
        scope=SCOPE_POD,
        query=(
            "100 * sum by (namespace, pod) "
            '(container_fs_usage_bytes{container!="",container!="POD"})'
            " / clamp_min("
            "sum by (namespace, pod) "
            '(container_fs_limit_bytes{container!="",container!="POD"}), 1)'
        ),
    ),
    MetricDefinition(
        field="db_latency_ms",
        scope=SCOPE_SERVICE,
        query=(
            "1000 * sum by (namespace, service) "
            "(rate(db_query_duration_seconds_sum[5m]))"
            " / clamp_min("
            "sum by (namespace, service) "
            "(rate(db_query_duration_seconds_count[5m])), 0.001)"
        ),
    ),
    MetricDefinition(
        field="db_connection_pool_percent",
        scope=SCOPE_SERVICE,
        query=(
            "100 * sum by (namespace, service) (db_pool_connections_used)"
            " / clamp_min(sum by (namespace, service) (db_pool_connections_max), 1)"
        ),
    ),
)


def load_metric_schema(path: str | None = None) -> tuple[MetricDefinition, ...]:
    """Return the metric schema, applying an optional JSON override file."""
    if not path:
        return DEFAULT_METRICS
    file_path = Path(path)
    if not file_path.exists():
        raise ConfigError(f"PROMETHEUS_METRIC_QUERIES_FILE not found: {file_path}")
    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {file_path}: {exc}") from exc
    return parse_metric_schema(raw)


def parse_metric_schema(raw: Any) -> tuple[MetricDefinition, ...]:
    if not isinstance(raw, list):
        raise ConfigError("metric queries must be a list of objects")
    definitions: list[MetricDefinition] = []
    seen: set[str] = set()
    for index, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ConfigError(f"metric query #{index} must be an object")
        field = str(entry.get("field") or "").strip()
        query = str(entry.get("query") or "").strip()
        scope = str(entry.get("scope") or SCOPE_SERVICE).strip().lower()
        if not field or not query:
            raise ConfigError(f"metric query #{index} needs both 'field' and 'query'")
        if field in KUBERNETES_FIELDS:
            raise ConfigError(f"metric field {field!r} comes from Kubernetes and cannot be overridden")
        if field in seen:
            raise ConfigError(f"duplicate metric field {field!r}")
        if scope not in VALID_SCOPES:
            raise ConfigError(f"metric {field!r} has invalid scope {scope!r}")
        seen.add(field)
        definitions.append(MetricDefinition(field=field, query=query, scope=scope))
    return tuple(definitions)


def fields(definitions: Iterable[MetricDefinition]) -> tuple[str, ...]:
    return tuple(definition.field for definition in definitions)
