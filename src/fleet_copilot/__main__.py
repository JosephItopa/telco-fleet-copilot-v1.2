"""CLI entry point: ``python -m fleet_copilot``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from typing import Optional, Sequence

from .collectors.kubernetes import KubernetesCollector
from .collectors.prometheus import PrometheusCollector
from .config import AppConfig
from .errors import ConfigError
from .metrics_schema import load_metric_schema
from .mocks import MOCK_CLUSTERS, MockKubernetesCollector, MockPrometheusCollector
from .producer import MetricsProducer, NullProducer
from .scanner import FleetScanner
from .service import FleetCopilotService

logger = logging.getLogger("fleet_copilot")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def build_service(
    config: AppConfig,
    *,
    dry_run: bool = False,
    echo: bool = False,
) -> FleetCopilotService:
    schema = load_metric_schema(config.prometheus.metric_queries_file)
    if config.mock_mode:
        clusters = config.clusters or MOCK_CLUSTERS
        kubernetes = MockKubernetesCollector()
        prometheus = MockPrometheusCollector()
    else:
        clusters = config.clusters
        kubernetes = KubernetesCollector(clusters, config.scan)
        prometheus = PrometheusCollector(
            schema,
            timeout_seconds=config.prometheus.timeout_seconds,
        )
    scanner = FleetScanner(kubernetes, prometheus, clusters, schema=schema)
    sink = NullProducer(config.kafka) if dry_run else MetricsProducer(config.kafka)
    return FleetCopilotService(config, scanner, sink, echo=echo)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="fleet-copilot",
        description="Scan every application on the configured clusters and publish metrics to Kafka.",
    )
    parser.add_argument("--once", action="store_true", help="run one scan, publish, then exit")
    parser.add_argument("--dry-run", action="store_true", help="scan without publishing to Kafka")
    parser.add_argument("--echo", action="store_true", help="print every record to stdout")
    parser.add_argument("--mock", action="store_true", help="force mock mode (no cluster needed)")
    return parser.parse_args(list(argv))


async def _run_until_signal(service: FleetCopilotService) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, service.request_stop)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    await service.run_forever()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.mock:
        os.environ["MOCK_MODE"] = "true"
    try:
        config = AppConfig.from_env()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(config.log_level)

    service = build_service(config, dry_run=args.dry_run, echo=args.echo)
    if args.once:
        result = asyncio.run(service.scan_and_publish())
        logger.info(
            "single scan complete: %d records in %.2fs",
            len(result.metrics),
            result.duration_seconds,
        )
        return 0
    asyncio.run(_run_until_signal(service))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
