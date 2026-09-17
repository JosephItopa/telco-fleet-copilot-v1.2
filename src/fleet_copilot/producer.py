"""Kafka producer for fleet metric records.

Records are keyed by ``cluster/namespace/service/pod`` so every pod's history
lands on a single partition (per-consumer ordering). A scan is produced as one
batch and flushed once, which keeps throughput high while bounding memory.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Protocol, Sequence

from .config import KafkaConfig
from .models import AppMetric

logger = logging.getLogger(__name__)


class _ProducerLike(Protocol):  # pragma: no cover - structural typing only
    def produce(self, topic: str, key: bytes, value: bytes, on_delivery=None) -> None: ...
    def poll(self, timeout: float = 0) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...


@dataclass
class ProducerStats:
    produced: int = 0
    delivered: int = 0
    failed: int = 0
    last_error: Optional[str] = None


class MetricsProducer:
    def __init__(
        self,
        config: KafkaConfig,
        *,
        producer_factory: Optional[Callable[[dict[str, Any]], _ProducerLike]] = None,
        serializer: Optional[Callable[[AppMetric], bytes]] = None,
    ) -> None:
        self._config = config
        self._factory = producer_factory or _default_producer_factory
        self._serializer = serializer or serialize_metric
        self._producer: Optional[_ProducerLike] = None
        self._stats = ProducerStats()

    @property
    def stats(self) -> ProducerStats:
        return self._stats

    @property
    def topic(self) -> str:
        return self._config.topic

    def start(self) -> None:
        if self._producer is None:
            self._producer = self._factory(self._config.to_confluent_config())

    def stop(self, timeout: Optional[float] = None) -> None:
        if self._producer is None:
            return
        self.flush(timeout)
        self._producer = None

    def send_batch(self, metrics: Sequence[AppMetric]) -> int:
        """Produce a whole scan and block until delivery is confirmed."""
        if not metrics:
            return 0
        self.start()
        assert self._producer is not None
        for metric in metrics:
            self._produce_one(metric)
        remaining = self._producer.flush(self._config.flush_timeout_seconds)
        if remaining:
            logger.warning("%d kafka messages still queued after flush", remaining)
        logger.info(
            "published %d records to %s (delivered=%d failed=%d)",
            len(metrics),
            self._config.topic,
            self._stats.delivered,
            self._stats.failed,
        )
        return len(metrics)

    async def send_batch_async(self, metrics: Sequence[AppMetric]) -> int:
        return await asyncio.to_thread(self.send_batch, metrics)

    def flush(self, timeout: Optional[float] = None) -> int:
        if self._producer is None:
            return 0
        return self._producer.flush(
            self._config.flush_timeout_seconds if timeout is None else timeout
        )

    def _produce_one(self, metric: AppMetric) -> None:
        assert self._producer is not None
        payload = self._serializer(metric)
        key = metric.kafka_key.encode("utf-8")
        while True:
            try:
                self._producer.produce(
                    self._config.topic,
                    key=key,
                    value=payload,
                    on_delivery=self._on_delivery,
                )
                self._stats.produced += 1
                self._producer.poll(0)
                return
            except BufferError:
                # Local queue is full: serve callbacks and retry.
                self._producer.poll(0.1)

    def _on_delivery(self, error: Any, _message: Any) -> None:
        if error is not None:
            self._stats.failed += 1
            self._stats.last_error = str(error)
            logger.error("kafka delivery failed: %s", error)
            return
        self._stats.delivered += 1


class NullProducer:
    """Dry-run sink that serializes records but does not talk to Kafka."""

    def __init__(self, config: KafkaConfig, *, echo: bool = False) -> None:
        self._config = config
        self._echo = echo
        self._stats = ProducerStats()
        self._records: list[bytes] = []

    @property
    def topic(self) -> str:
        return self._config.topic

    @property
    def stats(self) -> ProducerStats:
        return self._stats

    @property
    def records(self) -> list[bytes]:
        return list(self._records)

    def start(self) -> None:
        return None

    def stop(self, timeout: Optional[float] = None) -> None:
        return None

    def send_batch(self, metrics: Sequence[AppMetric]) -> int:
        for metric in metrics:
            payload = serialize_metric(metric)
            self._records.append(payload)
            self._stats.produced += 1
            self._stats.delivered += 1
            if self._echo:
                print(payload.decode("utf-8"), flush=True)
        return len(metrics)

    async def send_batch_async(self, metrics: Sequence[AppMetric]) -> int:
        return self.send_batch(metrics)


def serialize_metric(metric: AppMetric) -> bytes:
    return json.dumps(metric.to_dict(), separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _default_producer_factory(conf: dict[str, Any]) -> _ProducerLike:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:  # pragma: no cover - dependency present in prod
        raise RuntimeError(
            "the 'confluent-kafka' package is required to publish; pip install confluent-kafka"
        ) from exc
    return Producer(conf)


def encode_records(metrics: Iterable[AppMetric]) -> list[bytes]:
    """Utility used by tooling/tests to preview the exact wire bytes."""
    return [serialize_metric(metric) for metric in metrics]
