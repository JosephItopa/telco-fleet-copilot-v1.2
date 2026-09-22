from app.models import WorkloadTarget
from app.normalizer import MetricsNormalizer


def test_normalizer_merges_sources():
    target = WorkloadTarget(
        app_id="payments",
        namespace="prod",
        pod_name="payments-123",
        pod_ip="10.0.0.1",
        scrape_url="http://10.0.0.1:8080/metrics",
        labels={"app": "payments"},
    )

    event = MetricsNormalizer("cluster-01").build_event(
        target,
        {"cpu_usage_cores": 0.2},
        {"http_requests_total": 100.0},
    )

    assert event.app_id == "payments"
    assert event.metrics["cpu_usage_cores"] == 0.2
    assert event.metrics["http_requests_total"] == 100.0
    assert "kubernetes_metrics" in event.sources
    assert "prometheus" in event.sources
