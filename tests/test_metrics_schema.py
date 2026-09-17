from __future__ import annotations

import json

import pytest

from fleet_copilot.errors import ConfigError
from fleet_copilot.metrics_schema import (
    DEFAULT_METRICS,
    NUMERIC_FIELDS,
    VALID_SCOPES,
    fields,
    load_metric_schema,
    parse_metric_schema,
)


def test_default_schema_covers_every_numeric_field():
    assert set(fields(DEFAULT_METRICS)) == set(NUMERIC_FIELDS)
    assert all(definition.scope in VALID_SCOPES for definition in DEFAULT_METRICS)
    assert all("[5m]" in definition.query or "or" in definition.query for definition in DEFAULT_METRICS)


def test_load_metric_schema_from_file(tmp_path):
    path = tmp_path / "metrics.json"
    path.write_text(
        json.dumps(
            [
                {
                    "field": "request_rate",
                    "scope": "service",
                    "query": "sum by (namespace, service) (rate(istio_requests_total[5m]))",
                }
            ]
        )
    )
    schema = load_metric_schema(str(path))
    assert len(schema) == 1
    assert schema[0].field == "request_rate"


def test_load_metric_schema_missing_file():
    with pytest.raises(ConfigError):
        load_metric_schema("/nonexistent/metrics.json")


def test_kubernetes_owned_fields_cannot_be_overridden():
    with pytest.raises(ConfigError):
        parse_metric_schema([{"field": "pod_ready", "query": "up"}])


def test_invalid_scope_is_rejected():
    with pytest.raises(ConfigError):
        parse_metric_schema([{"field": "request_rate", "query": "up", "scope": "cluster"}])


def test_duplicate_fields_are_rejected():
    with pytest.raises(ConfigError):
        parse_metric_schema(
            [
                {"field": "request_rate", "query": "up"},
                {"field": "request_rate", "query": "up"},
            ]
        )


def test_missing_query_is_rejected():
    with pytest.raises(ConfigError):
        parse_metric_schema([{"field": "request_rate"}])


def test_schema_must_be_a_list():
    with pytest.raises(ConfigError):
        parse_metric_schema({"field": "request_rate"})
