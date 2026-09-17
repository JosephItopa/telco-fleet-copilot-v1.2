"""Environment-driven configuration.

All knobs are read once at startup. Secrets are never logged; ``KafkaConfig``
keeps credentials out of its ``repr``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .errors import ConfigError

DEFAULT_TOPIC = "fleet.app.metrics"
DEFAULT_INTERVAL_SECONDS = 180.0
DEFAULT_CONCURRENCY = 64
DEFAULT_DENY_NAMESPACES = ("kube-system", "kube-public", "kube-node-lease")


def _env(name: str) -> Optional[str]:
    raw = os.environ.get(name)
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


def _env_list(name: str) -> tuple[str, ...]:
    raw = _env(name)
    if raw is None:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True)
class ClusterConfig:
    """One Kubernetes cluster to scan, with its own Prometheus endpoint."""

    name: str
    kubeconfig: Optional[str] = None
    context: Optional[str] = None
    prometheus_url: Optional[str] = None
    verify_ssl: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], default_prometheus: Optional[str]) -> "ClusterConfig":
        name = str(raw.get("name") or "").strip()
        if not name:
            raise ConfigError("each cluster entry needs a non-empty 'name'")
        prometheus = raw.get("prometheus_url") or raw.get("prometheus") or default_prometheus
        return cls(
            name=name,
            kubeconfig=raw.get("kubeconfig") or None,
            context=raw.get("context") or None,
            prometheus_url=str(prometheus) if prometheus else None,
            verify_ssl=bool(raw.get("verify_ssl", True)),
        )


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str = DEFAULT_TOPIC
    client_id: str = "fleet-copilot-scanner"
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = field(default=None, repr=False)
    linger_ms: int = 50
    batch_size: int = 65536
    enable_idempotence: bool = True
    flush_timeout_seconds: float = 30.0

    def to_confluent_config(self) -> dict[str, Any]:
        conf: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            "enable.idempotence": self.enable_idempotence,
            "acks": "all" if self.enable_idempotence else "1",
            "compression.type": "zstd",
            "queue.buffering.max.messages": 1_000_000,
            "queue.buffering.max.kbytes": 1_048_576,
        }
        if self.security_protocol:
            conf["security.protocol"] = self.security_protocol
        if self.sasl_mechanism:
            conf["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            conf["sasl.username"] = self.sasl_username
        if self.sasl_password:
            conf["sasl.password"] = self.sasl_password
        return conf


@dataclass(frozen=True)
class ScanConfig:
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    concurrency: int = DEFAULT_CONCURRENCY
    namespace_allow: tuple[str, ...] = ()
    namespace_deny: tuple[str, ...] = DEFAULT_DENY_NAMESPACES
    require_owner: bool = True

    def namespace_allowed(self, namespace: str) -> bool:
        if self.namespace_allow and namespace not in self.namespace_allow:
            return False
        return namespace not in self.namespace_deny


@dataclass(frozen=True)
class PrometheusConfig:
    default_url: Optional[str] = None
    timeout_seconds: float = 20.0
    metric_queries_file: Optional[str] = None


@dataclass(frozen=True)
class AppConfig:
    kafka: KafkaConfig
    scan: ScanConfig
    prometheus: PrometheusConfig
    clusters: tuple[ClusterConfig, ...]
    mock_mode: bool = False
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "AppConfig":
        if env is not None:
            with _temporary_environ(env):
                return cls._from_process_env()
        return cls._from_process_env()

    @classmethod
    def _from_process_env(cls) -> "AppConfig":
        mock_mode = _env_bool("MOCK_MODE", False)
        prometheus = PrometheusConfig(
            default_url=_env("PROMETHEUS_URL"),
            timeout_seconds=_env_float("PROMETHEUS_TIMEOUT_SECONDS", 20.0),
            metric_queries_file=_env("PROMETHEUS_METRIC_QUERIES_FILE"),
        )
        scan = ScanConfig(
            interval_seconds=_env_float("SCAN_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS),
            concurrency=max(1, _env_int("SCAN_CONCURRENCY", DEFAULT_CONCURRENCY)),
            namespace_allow=_env_list("NAMESPACE_ALLOW"),
            namespace_deny=_env_list("NAMESPACE_DENY") or DEFAULT_DENY_NAMESPACES,
            require_owner=_env_bool("REQUIRE_OWNER", True),
        )
        kafka = KafkaConfig(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092",
            topic=_env("KAFKA_TOPIC") or DEFAULT_TOPIC,
            client_id=_env("KAFKA_CLIENT_ID") or "fleet-copilot-scanner",
            security_protocol=_env("KAFKA_SECURITY_PROTOCOL"),
            sasl_mechanism=_env("KAFKA_SASL_MECHANISM"),
            sasl_username=_env("KAFKA_SASL_USERNAME"),
            sasl_password=_env("KAFKA_SASL_PASSWORD"),
            linger_ms=_env_int("KAFKA_LINGER_MS", 50),
            batch_size=_env_int("KAFKA_BATCH_SIZE", 65536),
            enable_idempotence=_env_bool("KAFKA_ENABLE_IDEMPOTENCE", True),
            flush_timeout_seconds=_env_float("KAFKA_FLUSH_TIMEOUT_SECONDS", 30.0),
        )
        clusters = load_clusters(prometheus.default_url)
        if not clusters and not mock_mode:
            raise ConfigError(
                "no clusters configured: set CLUSTERS (JSON) or CLUSTERS_FILE, "
                "or run with MOCK_MODE=true"
            )
        if scan.interval_seconds <= 0:
            raise ConfigError("SCAN_INTERVAL_SECONDS must be > 0")
        return cls(
            kafka=kafka,
            scan=scan,
            prometheus=prometheus,
            clusters=clusters,
            mock_mode=mock_mode,
            log_level=(_env("LOG_LEVEL") or "INFO").upper(),
        )


def load_clusters(default_prometheus: Optional[str] = None) -> tuple[ClusterConfig, ...]:
    inline = _env("CLUSTERS")
    if inline:
        return _parse_clusters(_parse_json(inline, "CLUSTERS"), default_prometheus)
    path = _env("CLUSTERS_FILE")
    if path:
        document = _load_document(Path(path))
        return _parse_clusters(document, default_prometheus)
    return ()


def _parse_clusters(document: Any, default_prometheus: Optional[str]) -> tuple[ClusterConfig, ...]:
    if isinstance(document, Mapping):
        entries = document.get("clusters", [])
    else:
        entries = document
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise ConfigError("clusters configuration must be a list")
    clusters = tuple(
        ClusterConfig.from_mapping(entry, default_prometheus)
        for entry in entries
        if isinstance(entry, Mapping)
    )
    names = [cluster.name for cluster in clusters]
    if len(names) != len(set(names)):
        raise ConfigError("cluster names must be unique")
    return clusters


def _load_document(path: Path) -> Any:
    if not path.exists():
        raise ConfigError(f"CLUSTERS_FILE not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError:
        return _parse_json(text, str(path))
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:  # pragma: no cover - depends on PyYAML
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc


def _parse_json(text: str, source: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {source}: {exc}") from exc


class _temporary_environ:
    """Context manager that applies a mapping to ``os.environ`` and restores it."""

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)
        self._previous: dict[str, Optional[str]] = {}

    def __enter__(self) -> None:
        for key, value in self._values.items():
            self._previous[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    def __exit__(self, *exc_info: Any) -> None:
        for key, value in self._previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
