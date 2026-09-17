"""Reference consumer for the ``fleet.app.metrics`` stream.

Run::

    python -m fleet_copilot.consumer --from-beginning --limit 20

Subscribing a real service follows the same shape: since records are keyed by
``cluster/namespace/service/pod``, every pod's observations are ordered within
a partition. Use one consumer group per downstream system so Kafka fans the
topic out independently.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Optional, Sequence

logger = logging.getLogger("fleet_copilot.consumer")

THRESHOLDS = {
    "cpu_usage_percent": 80.0,
    "memory_usage_percent": 85.0,
    "cpu_throttling_percent": 10.0,
    "error_rate_percent": 5.0,
    "p95_latency_ms": 1000.0,
    "disk_usage_percent": 80.0,
    "db_latency_ms": 400.0,
    "db_connection_pool_percent": 85.0,
}


def alerting_fields(record: dict) -> list[str]:
    """Return the metric fields breaching their alert threshold."""
    breaches: list[str] = []
    for field, threshold in THRESHOLDS.items():
        value = record.get(field)
        if isinstance(value, (int, float)) and value >= threshold:
            breaches.append(f"{field}={value}")
    if record.get("pod_ready") is False:
        breaches.append("pod_ready=false")
    if record.get("liveness") is False:
        breaches.append("liveness=false")
    if record.get("readiness") is False:
        breaches.append("readiness=false")
    if isinstance(record.get("restart_count"), int) and record["restart_count"] >= 3:
        breaches.append(f"restart_count={record['restart_count']}")
    return breaches


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fleet-copilot-consumer")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="fleet.app.metrics")
    parser.add_argument("--group", default="fleet-copilot-demo")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="stop after N records (0 = forever)")
    parser.add_argument("--quiet", action="store_true", help="only print alerts")
    return parser.parse_args(list(argv))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

    try:
        from confluent_kafka import Consumer
    except ImportError:
        print("confluent-kafka is required: pip install confluent-kafka", file=sys.stderr)
        return 2

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([args.topic])
    logger.info("consuming %s from %s", args.topic, args.bootstrap)

    seen = 0
    alerts = 0
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("consume error: %s", message.error())
                continue
            seen += 1
            key = (message.key() or b"").decode("utf-8", "replace")
            record = json.loads(message.value())
            breaches = alerting_fields(record)
            if breaches:
                alerts += 1
                logger.warning("ALERT %s: %s", key, ", ".join(breaches))
            elif not args.quiet:
                logger.info(
                    "%s cpu=%.1f%% mem=%.1f%% err=%.2f%% p95=%.0fms",
                    key,
                    record.get("cpu_usage_percent") or 0.0,
                    record.get("memory_usage_percent") or 0.0,
                    record.get("error_rate_percent") or 0.0,
                    record.get("p95_latency_ms") or 0.0,
                )
            if args.limit and seen >= args.limit:
                logger.info("stopping after %d records (%d alerts)", seen, alerts)
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
