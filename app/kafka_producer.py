import json
import logging
from confluent_kafka import Producer
from .config import settings

LOG = logging.getLogger(__name__)


class PartitionedMetricsProducer:
    def __init__(self):
        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": settings.kafka_acks,
                "compression.type": settings.kafka_compression,
                "linger.ms": settings.kafka_linger_ms,
                "batch.size": settings.kafka_batch_size,
                "enable.idempotence": True,
            }
        )

    def publish(self, event: dict) -> None:
        key = event["app_id"]
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")

        self.producer.produce(
            topic=settings.kafka_topic,
            key=key.encode("utf-8"),
            value=payload,
            on_delivery=self._delivery_report,
        )

        # Serve delivery callbacks and avoid unbounded local buffering.
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        remaining = self.producer.flush(timeout)
        if remaining:
            LOG.error("%d Kafka messages remain undelivered", remaining)

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            LOG.error("Kafka delivery failed: %s", err)
        else:
            LOG.debug(
                "Published app=%s partition=%s offset=%s",
                msg.key().decode() if msg.key() else None,
                msg.partition(),
                msg.offset(),
            )
