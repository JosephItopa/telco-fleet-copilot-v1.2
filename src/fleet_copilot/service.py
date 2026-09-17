"""The long-running scan loop.

Every ``SCAN_INTERVAL_SECONDS`` (default 180s) the service runs one scan and
publishes the batch. The next scan starts on the interval boundary relative to
the *start* of the previous scan, so a slow scan does not push the schedule.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Protocol, Sequence

from .config import AppConfig
from .models import AppMetric
from .scanner import FleetScanner, ScanResult

logger = logging.getLogger(__name__)


class MetricsSink(Protocol):
    async def send_batch_async(self, metrics: Sequence[AppMetric]) -> int: ...


class FleetCopilotService:
    def __init__(
        self,
        config: AppConfig,
        scanner: FleetScanner,
        sink: MetricsSink,
        *,
        echo: bool = False,
    ) -> None:
        self._config = config
        self._scanner = scanner
        self._sink = sink
        self._echo = echo
        self._stop = asyncio.Event()
        self._last_result: Optional[ScanResult] = None

    @property
    def last_result(self) -> Optional[ScanResult]:
        return self._last_result

    def request_stop(self) -> None:
        self._stop.set()

    async def scan_and_publish(self) -> ScanResult:
        result = await self._scanner.scan_once()
        if self._echo:
            for metric in result.metrics:
                print(metric.to_dict(), flush=True)
        await self._sink.send_batch_async(result.metrics)
        self._last_result = result
        if result.errors:
            logger.warning("%d scan errors: %s", len(result.errors), "; ".join(result.errors))
        return result

    async def run_forever(self) -> None:
        interval = self._config.scan.interval_seconds
        logger.info(
            "fleet scanner started: interval=%.0fs clusters=%d topic=%s mock=%s",
            interval,
            len(self._config.clusters),
            self._config.kafka.topic,
            self._config.mock_mode,
        )
        while not self._stop.is_set():
            started = asyncio.get_running_loop().time()
            try:
                await self.scan_and_publish()
            except Exception:  # keep the loop alive across transient failures
                logger.exception("scan cycle failed")
            elapsed = asyncio.get_running_loop().time() - started
            delay = max(0.0, interval - elapsed)
            logger.info("next scan in %.1fs (cycle took %.1fs)", delay, elapsed)
            await self._sleep_or_stop(delay)
        logger.info("fleet scanner stopped")

    async def _sleep_or_stop(self, delay: float) -> None:
        if delay <= 0:
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
