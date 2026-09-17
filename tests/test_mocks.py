from __future__ import annotations

import asyncio

from fleet_copilot.collectors.prometheus import cluster_apps
from fleet_copilot.config import ClusterConfig
from fleet_copilot.metrics_schema import DEFAULT_METRICS
from fleet_copilot.mocks import MockKubernetesCollector, MockPrometheusCollector
from fleet_copilot.scanner import FleetScanner


def test_mock_fleet_is_deterministic_and_scales():
    collector = MockKubernetesCollector(apps=1200)
    first = asyncio.run(collector.collect_apps())
    second = asyncio.run(collector.collect_apps())
    assert len(first) == 1200
    assert [app.key for app in first] == [app.key for app in second]
    assert {app.cluster for app in first} == {"mock-eu", "mock-us"}


def test_mock_scanner_produces_full_records():
    clusters = (ClusterConfig(name="mock-eu"), ClusterConfig(name="mock-us"))
    scanner = FleetScanner(
        MockKubernetesCollector(apps=120),
        MockPrometheusCollector(),
        clusters,
        schema=DEFAULT_METRICS,
    )
    result = asyncio.run(scanner.scan_once())
    assert len(result.metrics) == 120
    assert all(metric.cpu_usage_percent is not None for metric in result.metrics)
    assert all(metric.p95_latency_ms is not None for metric in result.metrics)
    assert all(metric.disk_usage_percent is not None for metric in result.metrics)


def test_mock_metrics_group_by_cluster():
    apps = asyncio.run(MockKubernetesCollector(apps=40).collect_apps())
    grouped = cluster_apps(apps)
    indexes = asyncio.run(MockPrometheusCollector().collect(grouped, {}))
    assert set(indexes) == set(grouped)
    assert all(len(index) > 0 for index in indexes.values())
