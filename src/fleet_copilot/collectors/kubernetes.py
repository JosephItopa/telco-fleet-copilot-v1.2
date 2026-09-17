"""Kubernetes discovery: list every application pod on every configured cluster.

The Kubernetes client is synchronous, so each cluster is listed in a worker
thread. Each cluster gets its own ``ApiClient``/``Configuration`` so concurrent
scans never rely on the globally mutated kubernetes config.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Sequence

from ..config import ClusterConfig, ScanConfig
from ..errors import CollectionError
from ..models import SERVICE_LABEL_KEYS, AppRef
from ..timeutil import utcnow

logger = logging.getLogger(__name__)

# Waiting reasons that mean the container is not alive.
DEAD_WAITING_REASONS = frozenset(
    {
        "CrashLoopBackOff",
        "ErrImagePull",
        "ImagePullBackOff",
        "CreateContainerConfigError",
        "CreateContainerError",
        "InvalidImageName",
    }
)
DEAD_TERMINATED_REASONS = frozenset({"Error", "OOMKilled"})
SKIPPED_PHASES = frozenset({"Succeeded", "Failed"})


class KubernetesCollector:
    """Discovers application pods across a set of clusters."""

    def __init__(
        self,
        clusters: Sequence[ClusterConfig],
        scan: Optional[ScanConfig] = None,
        *,
        request_timeout_seconds: float = 60.0,
    ) -> None:
        self._clusters = tuple(clusters)
        self._scan = scan or ScanConfig()
        self._request_timeout = request_timeout_seconds

    async def collect_apps(self) -> list[AppRef]:
        results = await asyncio.gather(
            *(self._collect_cluster(cluster) for cluster in self._clusters),
            return_exceptions=True,
        )
        apps: list[AppRef] = []
        failures: list[str] = []
        for cluster, result in zip(self._clusters, results):
            if isinstance(result, BaseException):
                logger.error("cluster %s scan failed: %s", cluster.name, result)
                failures.append(f"{cluster.name}: {result}")
                continue
            logger.info("cluster %s: discovered %d application pods", cluster.name, len(result))
            apps.extend(result)
        if failures and not apps:
            raise CollectionError("all cluster scans failed: " + "; ".join(failures))
        return apps

    async def _collect_cluster(self, cluster: ClusterConfig) -> list[AppRef]:
        return await asyncio.to_thread(self._collect_cluster_sync, cluster)

    def _collect_cluster_sync(self, cluster: ClusterConfig) -> list[AppRef]:
        api = self._build_core_api(cluster)
        try:
            response = api.list_pod_for_all_namespaces(
                watch=False,
                _request_timeout=self._request_timeout,
            )
        except Exception as exc:  # pragma: no cover - network dependent
            raise CollectionError(f"cannot list pods on cluster {cluster.name!r}: {exc}") from exc

        now = utcnow()
        apps: list[AppRef] = []
        for pod in response.items or []:
            ref = self._to_app_ref(cluster.name, pod, now)
            if ref is not None:
                apps.append(ref)
        return apps

    def _build_core_api(self, cluster: ClusterConfig):
        try:
            from kubernetes import client, config  # imported lazily for testability
        except ImportError as exc:  # pragma: no cover - dependency present in prod
            raise CollectionError(
                "the 'kubernetes' package is required to scan clusters; pip install kubernetes"
            ) from exc

        configuration = client.Configuration()
        try:
            if cluster.kubeconfig:
                config.load_kube_config(
                    config_file=cluster.kubeconfig,
                    context=cluster.context,
                    client_configuration=configuration,
                )
            else:
                config.load_incluster_config(client_configuration=configuration)
        except Exception as exc:
            raise CollectionError(
                f"cannot load credentials for cluster {cluster.name!r}: {exc}"
            ) from exc
        configuration.verify_ssl = cluster.verify_ssl
        configuration.connection_pool_maxsize = max(10, self._scan.concurrency)
        return client.CoreV1Api(api_client=client.ApiClient(configuration=configuration))

    def _to_app_ref(self, cluster_name: str, pod, now) -> Optional[AppRef]:
        metadata = pod.metadata
        if metadata is None or metadata.deletion_timestamp is not None:
            return None

        namespace = metadata.namespace or "default"
        if not self._scan.namespace_allowed(namespace):
            return None
        if self._scan.require_owner and not metadata.owner_references:
            return None

        status = pod.status
        phase = getattr(status, "phase", None)
        if phase in SKIPPED_PHASES:
            return None

        labels = dict(metadata.labels or {})
        container_statuses = list(getattr(status, "container_statuses", None) or [])
        restart_count = sum(int(cs.restart_count or 0) for cs in container_statuses)

        ready_condition = _condition_true(status, "Ready")
        if container_statuses:
            readiness = all(bool(cs.ready) for cs in container_statuses)
        else:
            readiness = bool(ready_condition)
        pod_ready = bool(ready_condition)

        liveness = _is_live(phase, container_statuses)

        return AppRef(
            cluster=cluster_name,
            namespace=namespace,
            service=_resolve_service_name(metadata.name, labels, pod),
            pod=metadata.name,
            timestamp=now,
            restart_count=restart_count,
            pod_ready=pod_ready,
            liveness=liveness,
            readiness=readiness,
            labels={key: labels[key] for key in SERVICE_LABEL_KEYS if labels.get(key)},
        )


def _condition_true(status, condition_type: str) -> bool:
    for condition in getattr(status, "conditions", None) or []:
        if condition.type == condition_type:
            return (condition.status or "").lower() == "true"
    return False


def _is_live(phase: Optional[str], container_statuses) -> bool:
    if phase not in {"Running", "Succeeded", None}:
        return False
    for container in container_statuses:
        state = getattr(container, "state", None)
        if state is None:
            continue
        waiting = getattr(state, "waiting", None)
        if waiting is not None and waiting.reason in DEAD_WAITING_REASONS:
            return False
        terminated = getattr(state, "terminated", None)
        if terminated is not None and terminated.reason in DEAD_TERMINATED_REASONS:
            return False
    return True


def _resolve_service_name(pod_name: str, labels: dict[str, str], pod) -> str:
    for key in SERVICE_LABEL_KEYS:
        value = labels.get(key)
        if value:
            return value
    spec = getattr(pod, "spec", None)
    containers = list(getattr(spec, "containers", None) or [])
    if containers and getattr(containers[0], "name", None):
        return containers[0].name
    return pod_name
