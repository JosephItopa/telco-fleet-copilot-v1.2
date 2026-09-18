from __future__ import annotations

import asyncio
from typing import Mapping, Sequence

from fleet_copilot.collectors.prometheus import MetricsIndex
from fleet_copilot.scanner import FleetScanner
from helpers import NOW, make_app_ref, make_cluster


class FakeKube:
    def __init__(self, apps=None, error: Exception | None = None):
        self._apps = apps or []
        self._error = error
        self.calls = 0

    async def collect_apps(self):
        self.calls += 1
        if self._error:
            raise self._error
        return list(self._apps)


class FakeProm:
    def __init__(self, indexes: Mapping[str, MetricsIndex] | None = None, error=None):
        self._indexes = dict(indexes or {})
        self._error = error
        self.calls = 0
        self.seen: dict[str, list] = {}

    async def collect(self, apps_by_cluster, clusters):
        self.calls += 1
        self.seen = {name: list(apps) for name, apps in apps_by_cluster.items()}
        if self._error:
            raise self._error
        return self._indexes


def make_scanner(kube, prom):
    clusters = [make_cluster("prod-eu"), make_cluster("prod-us")]
    return FleetScanner(kube, prom, clusters, clock=lambda: NOW)


def test_scan_merges_kubernetes_state_with_prometheus_metrics():
    app = make_app_ref(cluster="prod-eu")
    other = make_app_ref(cluster="prod-us", namespace="core", service="auth-service", pod="auth-1")

    index = MetricsIndex()
    index.add_pod("cpu_usage_percent", "production", "payment-api-7d9f8", 82.4)
    index.add_service("request_rate", "production", "payment-api", 245.0)
    index.add_pod("disk_usage_percent", "production", "payment-api-7d9f8", 76.2)

    scanner = make_scanner(FakeKube([app, other]), FakeProm({"prod-eu": index}))
    result = asyncio.run(scanner.scan_once())

    assert result.errors == []
    assert len(result.metrics) == 2
    payment = next(metric for metric in result.metrics if metric.service == "payment-api")
    assert payment.cpu_usage_percent == 82.4
    assert payment.request_rate == 245.0
    assert payment.disk_usage_percent == 76.2
    assert payment.memory_usage_percent is None
    assert payment.restart_count == 3
    assert payment.pod_ready is True
    assert payment.cluster == "prod-eu"
    assert payment.timestamp == "2026-09-17T12:00:00Z"

    auth = next(metric for metric in result.metrics if metric.service == "auth-service")
    assert auth.cluster == "prod-us"
    assert auth.cpu_usage_percent is None


def test_metrics_are_sorted_by_key_for_stable_batches():
    apps = [
        make_app_ref(cluster="prod-us", service="z", pod="z-1"),
        make_app_ref(cluster="prod-eu", service="a", pod="a-1"),
    ]
    scanner = make_scanner(FakeKube(apps), FakeProm())
    result = asyncio.run(scanner.scan_once())
    keys = [metric.kafka_key for metric in result.metrics]
    assert keys == sorted(keys)


def test_discovery_failure_yields_no_metrics_and_an_error():
    scanner = make_scanner(FakeKube(error=RuntimeError("boom")), FakeProm())
    result = asyncio.run(scanner.scan_once())
    assert result.metrics == []
    assert any("pod discovery failed" in error for error in result.errors)


def test_metrics_failure_degrades_to_null_but_keeps_state():
    app = make_app_ref()
    scanner = make_scanner(FakeKube([app]), FakeProm(error=RuntimeError("prom down")))
    result = asyncio.run(scanner.scan_once())

    assert any("metrics collection failed" in error for error in result.errors)
    assert len(result.metrics) == 1
    assert result.metrics[0].cpu_usage_percent is None
    assert result.metrics[0].restart_count == 3


def test_no_apps_skips_prometheus():
    prom = FakeProm()
    scanner = make_scanner(FakeKube([]), prom)
    result = asyncio.run(scanner.scan_once())
    assert prom.calls == 0
    assert result.metrics == []


def test_cluster_counts(  # sanity check helper used by logging
):
    apps: Sequence = [make_app_ref(cluster="a"), make_app_ref(cluster="b")]
    scanner = FleetScanner(FakeKube(apps), FakeProm(), [make_cluster("a"), make_cluster("b")])
    result = asyncio.run(scanner.scan_once())
    assert result.cluster_counts == {"a": 1, "b": 1}
    assert result.duration_seconds >= 0
