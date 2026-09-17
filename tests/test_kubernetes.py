from __future__ import annotations

from fleet_copilot.collectors.kubernetes import KubernetesCollector, _is_live
from fleet_copilot.config import ScanConfig
from helpers import NOW, make_pod


def make_collector(scan: ScanConfig | None = None) -> KubernetesCollector:
    return KubernetesCollector([], scan or ScanConfig())


def test_running_pod_maps_to_app_ref():
    collector = make_collector()
    ref = collector._to_app_ref("prod-eu", make_pod(), NOW)
    assert ref is not None
    assert ref.cluster == "prod-eu"
    assert ref.namespace == "production"
    assert ref.service == "payment-api"
    assert ref.pod == "payment-api-7d9f8"
    assert ref.restart_count == 3
    assert ref.pod_ready is True
    assert ref.readiness is True
    assert ref.liveness is True
    assert ref.timestamp == NOW


def test_not_ready_pod_reports_readiness_false():
    collector = make_collector()
    ref = collector._to_app_ref("prod-eu", make_pod(ready=False), NOW)
    assert ref is not None
    assert ref.pod_ready is False
    assert ref.readiness is False


def test_crash_loop_backoff_is_not_live():
    collector = make_collector()
    ref = collector._to_app_ref(
        "prod-eu", make_pod(waiting_reason="CrashLoopBackOff"), NOW
    )
    assert ref is not None
    assert ref.liveness is False


def test_pending_phase_is_not_live():
    assert _is_live("Pending", []) is False
    assert _is_live("Running", []) is True


def test_deleting_pod_is_ignored():
    collector = make_collector()
    ref = collector._to_app_ref("prod-eu", make_pod(deletion_timestamp="now"), NOW)
    assert ref is None


def test_completed_pod_is_ignored():
    collector = make_collector()
    assert collector._to_app_ref("prod-eu", make_pod(phase="Succeeded"), NOW) is None


def test_unmanaged_pod_ignored_unless_configured_otherwise():
    collector = make_collector()
    assert collector._to_app_ref("prod-eu", make_pod(owner=False), NOW) is None

    permissive = make_collector(ScanConfig(require_owner=False))
    assert permissive._to_app_ref("prod-eu", make_pod(owner=False), NOW) is not None


def test_namespace_denied_pod_is_ignored():
    collector = make_collector(ScanConfig(namespace_deny=("production",)))
    assert collector._to_app_ref("prod-eu", make_pod(), NOW) is None


def test_namespace_allowlist():
    collector = make_collector(ScanConfig(namespace_allow=("payments",)))
    assert collector._to_app_ref("prod-eu", make_pod(), NOW) is None
    ref = collector._to_app_ref("prod-eu", make_pod(namespace="payments"), NOW)
    assert ref is not None


def test_service_name_prefers_labels_then_container():
    collector = make_collector()
    labelled = collector._to_app_ref(
        "prod-eu", make_pod(labels={"app.kubernetes.io/name": "billing"}), NOW
    )
    assert labelled is not None and labelled.service == "billing"

    bare = collector._to_app_ref("prod-eu", make_pod(labels={}), NOW)
    assert bare is not None and bare.service == "payment-api"


def test_container_statuses_missing_falls_back_to_ready_condition():
    collector = make_collector()
    pod = make_pod()
    pod.status.container_statuses = []
    ref = collector._to_app_ref("prod-eu", pod, NOW)
    assert ref is not None
    assert ref.readiness is True
    assert ref.restart_count == 0


def test_labels_subset_is_retained_for_prometheus_resolution():
    collector = make_collector()
    pod = make_pod(labels={"app": "payment-api", "unrelated": "x", "app.kubernetes.io/name": "payment"})
    ref = collector._to_app_ref("prod-eu", pod, NOW)
    assert ref is not None
    assert ref.labels == {"app": "payment-api", "app.kubernetes.io/name": "payment"}
