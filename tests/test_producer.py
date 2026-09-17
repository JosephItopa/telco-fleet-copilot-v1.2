from __future__ import annotations

import json

from fleet_copilot.config import KafkaConfig
from fleet_copilot.models import AppMetric
from fleet_copilot.producer import MetricsProducer, NullProducer
from helpers import make_app_ref


class FakeKafkaProducer:
    def __init__(self, conf, *, fail_first: bool = False):
        self.conf = conf
        self.messages: list[dict] = []
        self.flushed = 0
        self.polls = 0
        self._fail_first = fail_first
        self._attempts = 0

    def produce(self, topic, key, value, on_delivery=None):
        self._attempts += 1
        if self._fail_first and self._attempts == 1:
            raise BufferError("queue full")
        self.messages.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None, None)

    def poll(self, timeout: float = 0) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float = 0) -> int:
        self.flushed += 1
        return 0


class FailingProducer(FakeKafkaProducer):
    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery("delivery failed", None)


def make_metric(service: str = "payment-api") -> AppMetric:
    return AppMetric.from_ref(
        make_app_ref(service=service),
        values={"cpu_usage_percent": 82.4, "request_rate": 245.0},
    )


def test_send_batch_keys_and_serializes_records():
    created: list[FakeKafkaProducer] = []

    def factory(conf):
        producer = FakeKafkaProducer(conf)
        created.append(producer)
        return producer

    producer = MetricsProducer(KafkaConfig(bootstrap_servers="broker:9092"), producer_factory=factory)
    count = producer.send_batch([make_metric(), make_metric("auth-service")])

    assert count == 2
    fake = created[0]
    assert fake.flushed == 1
    assert all(message["topic"] == "fleet.app.metrics" for message in fake.messages)
    assert fake.messages[0]["key"] == b"prod-eu/production/payment-api/payment-api-7d9f8"

    payload = json.loads(fake.messages[0]["value"])
    assert payload["service"] == "payment-api"
    assert payload["cpu_usage_percent"] == 82.4
    assert payload["request_rate"] == 245.0
    assert payload["cluster"] == "prod-eu"

    assert producer.stats.produced == 2
    assert producer.stats.delivered == 2
    assert producer.stats.failed == 0


def test_empty_batch_does_not_start_a_producer():
    calls = []

    def factory(conf):
        calls.append(conf)
        return FakeKafkaProducer(conf)

    producer = MetricsProducer(KafkaConfig(bootstrap_servers="broker:9092"), producer_factory=factory)
    assert producer.send_batch([]) == 0
    assert calls == []


def test_buffer_error_is_retried():
    def factory(conf):
        return FakeKafkaProducer(conf, fail_first=True)

    producer = MetricsProducer(KafkaConfig(bootstrap_servers="broker:9092"), producer_factory=factory)
    producer.send_batch([make_metric()])
    assert producer.stats.delivered == 1


def test_delivery_failure_is_counted():
    def factory(conf):
        return FailingProducer(conf)

    producer = MetricsProducer(KafkaConfig(bootstrap_servers="broker:9092"), producer_factory=factory)
    producer.send_batch([make_metric()])
    assert producer.stats.failed == 1
    assert producer.stats.delivered == 0
    assert "delivery failed" in (producer.stats.last_error or "")


def test_null_producer_records_without_kafka(capsys):
    sink = NullProducer(KafkaConfig(bootstrap_servers="unused"), echo=True)
    assert sink.send_batch([make_metric()]) == 1
    captured = capsys.readouterr()
    assert "payment-api" in captured.out
    assert json.loads(sink.records[0])["cluster"] == "prod-eu"
    assert sink.stats.delivered == 1
