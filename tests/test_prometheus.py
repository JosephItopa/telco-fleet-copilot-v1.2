from __future__ import annotations

import asyncio

import pytest

from fleet_copilot.collectors.prometheus import MetricsIndex, PrometheusCollector, cluster_apps
from fleet_copilot.errors import CollectionError
from fleet_copilot.metrics_schema import DEFAULT_METRICS
from helpers import FakeSession, make_app_ref, make_cluster, vector_payload


def test_index_keys_by_pod_and_service():
    index = MetricsIndex()
    index.add_series(
        "cpu_usage_percent",
        {"namespace": "production", "pod": "payment-api-7d9f8", "service": "payment-api"},
        82.4,
    )
    assert index.get_pod("cpu_usage_percent", "production", "payment-api-7d9f8") == 82.4
    assert index.get_service("cpu_usage_percent", "production", "payment-api") == 82.4


def test_index_falls_back_through_application_labels():
    index = MetricsIndex()
    index.add_series("request_rate", {"namespace": "core", "app_kubernetes_io_name": "auth"}, 12.5)
    app = make_app_ref(namespace="core", service="auth", labels={"app.kubernetes.io/name": "auth"})
    assert index.resolve("request_rate", "service", app) == 12.5


def test_service_scope_falls_back_to_pod_scope():
    index = MetricsIndex()
    index.add_series("request_rate", {"namespace": "production", "pod": "pod-1"}, 44.0)
    app = make_app_ref(namespace="production", pod="pod-1", service="payment-api")
    assert index.resolve("request_rate", "service", app) == 44.0
    assert index.resolve("request_rate", "pod", app) == 44.0


def test_pod_scope_does_not_use_service_values():
    index = MetricsIndex()
    index.add_service("cpu_usage_percent", "production", "payment-api", 90.0)
    app = make_app_ref(namespace="production", pod="unrelated", service="payment-api")
    assert index.resolve("cpu_usage_percent", "pod", app) is None


def test_parse_response_valid_vector():
    payload = vector_payload([({"namespace": "ns", "pod": "p", "service": "s"}, "82.4")])
    parsed = PrometheusCollector.parse_response(payload)
    assert parsed[0][1] == 82.4


def test_parse_response_rejects_error_status():
    with pytest.raises(CollectionError):
        PrometheusCollector.parse_response({"status": "error", "error": "bad query"})


def test_parse_response_skips_nan_and_histograms():
    payload = {
        "status": "success",
        "data": {
            "result": [
                {"metric": {"pod": "a"}, "value": [1.0, "NaN"]},
                {"metric": {"pod": "b"}, "value": [1.0, {"count": "1"}]},
                {"metric": {"pod": "c"}, "value": [1.0, "3.5"]},
            ]
        },
    }
    parsed = PrometheusCollector.parse_response(payload)
    assert parsed == [({"pod": "c"}, 3.5)]


def test_collect_cluster_queries_once_per_metric():
    collector = PrometheusCollector(DEFAULT_METRICS, timeout_seconds=1.0)
    session = FakeSession(
        vector_payload([({"namespace": "production", "pod": "pod-1"}, "91.2")])
    )
    index = asyncio.run(
        collector._collect_cluster(session, "prod-eu", make_cluster(url="http://prom"))
    )
    assert len(session.queries) == len(DEFAULT_METRICS)
    assert index.get_pod("memory_usage_percent", "production", "pod-1") == 91.2


def test_collect_cluster_isolates_failed_query():
    collector = PrometheusCollector(DEFAULT_METRICS, timeout_seconds=1.0)
    session = FakeSession({"status": "error", "error": "bad query"})
    index = asyncio.run(
        collector._collect_cluster(session, "prod-eu", make_cluster(url="http://prom"))
    )
    assert len(index) == 0


def test_collect_skips_clusters_without_prometheus_url():
    app = make_app_ref()
    collector = PrometheusCollector(DEFAULT_METRICS)
    indexes = asyncio.run(
        collector.collect({"prod-eu": [app]}, {"prod-eu": make_cluster(url=None)})
    )
    assert indexes["prod-eu"].resolve("request_rate", "service", app) is None


def test_cluster_apps_groups_by_cluster():
    grouped = cluster_apps(
        [make_app_ref(cluster="a"), make_app_ref(cluster="b"), make_app_ref(cluster="a")]
    )
    assert {name: len(apps) for name, apps in grouped.items()} == {"a": 2, "b": 1}
