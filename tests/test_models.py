from __future__ import annotations

import json

from fleet_copilot.models import AppMetric
from helpers import make_app_ref

EXPECTED_KEYS = [
    "timestamp",
    "service",
    "namespace",
    "pod",
    "cpu_usage_percent",
    "memory_usage_percent",
    "cpu_throttling_percent",
    "request_rate",
    "error_rate_percent",
    "p95_latency_ms",
    "active_requests",
    "restart_count",
    "pod_ready",
    "disk_usage_percent",
    "db_latency_ms",
    "db_connection_pool_percent",
    "liveness",
    "readiness",
    "cluster",
]


def test_wire_contract_key_order_and_json_roundtrip():
    ref = make_app_ref()
    metric = AppMetric.from_ref(
        ref,
        values={
            "cpu_usage_percent": 82.402,
            "memory_usage_percent": 91.2,
            "cpu_throttling_percent": 14.3,
            "request_rate": 245.0,
            "error_rate_percent": 7.8,
            "p95_latency_ms": 1240.0,
            "active_requests": 53.4,
            "disk_usage_percent": 76.2,
            "db_latency_ms": 480.0,
            "db_connection_pool_percent": 87.0,
        },
    )

    payload = metric.to_dict()
    assert list(payload.keys()) == EXPECTED_KEYS
    assert payload["timestamp"] == "2026-09-17T12:00:00Z"
    assert payload["service"] == "payment-api"
    assert payload["cluster"] == "prod-eu"
    assert payload["active_requests"] == 53
    assert payload["cpu_usage_percent"] == 82.402
    assert payload["restart_count"] == 3
    assert payload["pod_ready"] is True

    assert json.loads(json.dumps(payload)) == payload


def test_missing_metrics_are_null_not_zero():
    metric = AppMetric.from_ref(make_app_ref(), values={})
    payload = metric.to_dict()
    assert payload["cpu_usage_percent"] is None
    assert payload["db_latency_ms"] is None
    assert payload["active_requests"] is None
    assert payload["restart_count"] == 3
    assert payload["liveness"] is True


def test_non_finite_and_garbage_values_are_dropped():
    metric = AppMetric.from_ref(
        make_app_ref(),
        values={"cpu_usage_percent": float("nan"), "memory_usage_percent": "n/a"},
    )
    assert metric.cpu_usage_percent is None
    assert metric.memory_usage_percent is None


def test_kafka_key_is_pod_scoped():
    assert make_app_ref().key == "prod-eu/production/payment-api/payment-api-7d9f8"
    metric = AppMetric.from_ref(make_app_ref())
    assert metric.kafka_key == "prod-eu/production/payment-api/payment-api-7d9f8"
