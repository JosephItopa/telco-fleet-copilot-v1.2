from __future__ import annotations

import json

import pytest

from fleet_copilot.config import AppConfig
from fleet_copilot.errors import ConfigError

ENV_KEYS = [
    "MOCK_MODE",
    "CLUSTERS",
    "CLUSTERS_FILE",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "KAFKA_CLIENT_ID",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_ENABLE_IDEMPOTENCE",
    "SCAN_INTERVAL_SECONDS",
    "SCAN_CONCURRENCY",
    "NAMESPACE_ALLOW",
    "NAMESPACE_DENY",
    "REQUIRE_OWNER",
    "PROMETHEUS_URL",
    "PROMETHEUS_TIMEOUT_SECONDS",
    "PROMETHEUS_METRIC_QUERIES_FILE",
    "LOG_LEVEL",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_missing_clusters_is_a_config_error():
    with pytest.raises(ConfigError, match="no clusters configured"):
        AppConfig.from_env({})


def test_mock_mode_needs_no_clusters():
    config = AppConfig.from_env({"MOCK_MODE": "true"})
    assert config.mock_mode is True
    assert config.clusters == ()
    assert config.scan.interval_seconds == 180.0


def test_inline_clusters_json_and_defaults():
    config = AppConfig.from_env(
        {
            "CLUSTERS": json.dumps(
                [
                    {"name": "prod-eu", "prometheus_url": "https://prom.eu"},
                    {"name": "prod-us", "context": "prod-us", "verify_ssl": False},
                ]
            ),
            "PROMETHEUS_URL": "https://prom.shared",
        }
    )
    assert [cluster.name for cluster in config.clusters] == ["prod-eu", "prod-us"]
    assert config.clusters[0].prometheus_url == "https://prom.eu"
    assert config.clusters[1].prometheus_url == "https://prom.shared"
    assert config.clusters[1].verify_ssl is False


def test_clusters_file_yaml(tmp_path):
    path = tmp_path / "clusters.yaml"
    path.write_text(json.dumps({"clusters": [{"name": "edge", "prometheus_url": "https://p.edge"}]}))
    config = AppConfig.from_env({"CLUSTERS_FILE": str(path)})
    assert config.clusters[0].name == "edge"


def test_missing_clusters_file():
    with pytest.raises(ConfigError, match="CLUSTERS_FILE not found"):
        AppConfig.from_env({"CLUSTERS_FILE": "/nope/clusters.yaml"})


def test_duplicate_cluster_names_rejected():
    with pytest.raises(ConfigError, match="unique"):
        AppConfig.from_env(
            {"CLUSTERS": json.dumps([{"name": "a"}, {"name": "a"}])}
        )


def test_namespace_filters():
    config = AppConfig.from_env(
        {
            "MOCK_MODE": "true",
            "NAMESPACE_ALLOW": "production, payments",
            "NAMESPACE_DENY": "staging",
        }
    )
    assert config.scan.namespace_allowed("production") is True
    assert config.scan.namespace_allowed("payments") is True
    assert config.scan.namespace_allowed("staging") is False
    assert config.scan.namespace_allowed("default") is False


def test_namespace_deny_defaults_when_empty():
    config = AppConfig.from_env({"MOCK_MODE": "true", "NAMESPACE_DENY": ""})
    assert config.scan.namespace_allowed("kube-system") is False
    assert config.scan.namespace_allowed("production") is True


def test_kafka_conf_idempotence_and_sasl():
    config = AppConfig.from_env(
        {
            "MOCK_MODE": "true",
            "KAFKA_BOOTSTRAP_SERVERS": "broker-1:9092",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "KAFKA_SASL_USERNAME": "scanner",
            "KAFKA_SASL_PASSWORD": "top-secret",
        }
    )
    conf = config.kafka.to_confluent_config()
    assert conf["bootstrap.servers"] == "broker-1:9092"
    assert conf["acks"] == "all"
    assert conf["enable.idempotence"] is True
    assert conf["sasl.username"] == "scanner"
    assert "top-secret" not in repr(config.kafka)


def test_invalid_interval_is_rejected():
    with pytest.raises(ConfigError):
        AppConfig.from_env({"MOCK_MODE": "true", "SCAN_INTERVAL_SECONDS": "0"})


def test_invalid_number_is_rejected():
    with pytest.raises(ConfigError, match="SCAN_INTERVAL_SECONDS"):
        AppConfig.from_env({"MOCK_MODE": "true", "SCAN_INTERVAL_SECONDS": "soon"})
