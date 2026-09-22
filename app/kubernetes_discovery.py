import logging
from kubernetes import client, config
from .models import WorkloadTarget

LOG = logging.getLogger(__name__)


class KubernetesDiscovery:
    def __init__(self, namespace: str | None = None):
        try:
            config.load_incluster_config()
            LOG.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            config.load_kube_config()
            LOG.info("Loaded local kubeconfig")

        self.core = client.CoreV1Api()
        self.namespace = namespace

    def discover(self) -> list[WorkloadTarget]:
        pods = (
            self.core.list_namespaced_pod(self.namespace).items
            if self.namespace
            else self.core.list_pod_for_all_namespaces().items
        )

        targets: list[WorkloadTarget] = []

        for pod in pods:
            if pod.status.phase != "Running":
                continue

            annotations = (pod.metadata.annotations or {})
            labels = pod.metadata.labels or {}

            if annotations.get("metrics.scrape/enabled", "").lower() != "true":
                continue

            port = annotations.get("metrics.scrape/port")
            path = annotations.get("metrics.scrape/path", "/metrics")
            if not port or not pod.status.pod_ip:
                continue

            # Support a numeric port. Named ports can be resolved from containers.
            if not port.isdigit():
                resolved = None
                for container in pod.spec.containers or []:
                    for container_port in container.ports or []:
                        if container_port.name == port:
                            resolved = container_port.container_port
                            break
                if resolved is None:
                    continue
                port = str(resolved)

            url = f"http://{pod.status.pod_ip}:{port}{path}"
            app_id = (
                labels.get("app.kubernetes.io/name")
                or labels.get("app")
                or pod.metadata.name
            )

            targets.append(
                WorkloadTarget(
                    app_id=app_id,
                    namespace=pod.metadata.namespace,
                    pod_name=pod.metadata.name,
                    pod_ip=pod.status.pod_ip,
                    scrape_url=url,
                    labels=labels,
                )
            )

        LOG.info("Discovered %d scrape targets", len(targets))
        return targets
