from typing import Any
from .models import MetricsEvent, WorkloadTarget


class MetricsNormalizer:
    def __init__(self, cluster: str):
        self.cluster = cluster

    def build_event(
        self,
        target: WorkloadTarget,
        kubernetes_metrics: dict[str, Any],
        prometheus_metrics: dict[str, Any],
    ) -> MetricsEvent:
        metrics = {
            **kubernetes_metrics,
            **prometheus_metrics,
        }

        sources = []
        if kubernetes_metrics:
            sources.append("kubernetes_metrics")
        if prometheus_metrics:
            sources.append("prometheus")

        return MetricsEvent.now(
            app_id=target.app_id,
            namespace=target.namespace,
            pod=target.pod_name,
            cluster=self.cluster,
            sources=sources,
            metrics=metrics,
            labels=target.labels,
        )
