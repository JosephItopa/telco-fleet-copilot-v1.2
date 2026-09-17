from __future__ import annotations

from fleet_copilot.consumer import alerting_fields


def healthy_record() -> dict:
    return {
        "cpu_usage_percent": 20.0,
        "memory_usage_percent": 30.0,
        "cpu_throttling_percent": 0.0,
        "error_rate_percent": 0.1,
        "p95_latency_ms": 120.0,
        "disk_usage_percent": 40.0,
        "db_latency_ms": 30.0,
        "db_connection_pool_percent": 20.0,
        "restart_count": 0,
        "pod_ready": True,
        "liveness": True,
        "readiness": True,
    }


def test_healthy_record_has_no_alerts():
    assert alerting_fields(healthy_record()) == []


def test_threshold_breaches_are_reported():
    record = healthy_record()
    record.update({"cpu_usage_percent": 82.4, "error_rate_percent": 7.8, "p95_latency_ms": 1240})
    breaches = alerting_fields(record)
    assert any(item.startswith("cpu_usage_percent") for item in breaches)
    assert any(item.startswith("error_rate_percent") for item in breaches)
    assert any(item.startswith("p95_latency_ms") for item in breaches)


def test_boolean_and_restart_scoreboards_are_reported():
    record = healthy_record()
    record.update({"pod_ready": False, "liveness": False, "readiness": False, "restart_count": 5})
    breaches = alerting_fields(record)
    assert "pod_ready=false" in breaches
    assert "liveness=false" in breaches
    assert "readiness=false" in breaches
    assert "restart_count=5" in breaches


def test_missing_metrics_are_ignored():
    assert alerting_fields({}) == []
