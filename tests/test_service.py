from __future__ import annotations

import asyncio

from fleet_copilot.models import AppMetric
from fleet_copilot.scanner import ScanResult
from fleet_copilot.service import FleetCopilotService
from helpers import NOW, make_app_ref, make_config


def make_metric() -> AppMetric:
    return AppMetric.from_ref(make_app_ref(), values={"cpu_usage_percent": 42.0})


class FakeScanner:
    def __init__(self, metrics=None, error: Exception | None = None):
        self._metrics = metrics if metrics is not None else [make_metric()]
        self._error = error
        self.calls = 0

    async def scan_once(self) -> ScanResult:
        self.calls += 1
        if self._error:
            raise self._error
        return ScanResult(started_at=NOW, finished_at=NOW, metrics=list(self._metrics))


class CountingSink:
    def __init__(self):
        self.calls = 0
        self.received: list[list[AppMetric]] = []

    async def send_batch_async(self, metrics):
        self.calls += 1
        self.received.append(list(metrics))
        return len(metrics)


def test_scan_and_publish_records_result():
    scanner = FakeScanner()
    sink = CountingSink()
    service = FleetCopilotService(make_config(), scanner, sink)

    result = asyncio.run(service.scan_and_publish())

    assert scanner.calls == 1
    assert sink.calls == 1
    assert sink.received[0][0].service == "payment-api"
    assert service.last_result is result


def test_run_forever_scans_repeatedly_until_stopped():
    scanner = FakeScanner()
    sink = CountingSink()
    service = FleetCopilotService(make_config(interval=0.05), scanner, sink)

    async def stopper():
        while sink.calls < 2:
            await asyncio.sleep(0.01)
        service.request_stop()

    async def run():
        await asyncio.wait_for(
            asyncio.gather(service.run_forever(), stopper()),
            timeout=5.0,
        )

    asyncio.run(run())
    assert sink.calls >= 2
    assert scanner.calls >= 2


def test_scan_failure_does_not_kill_the_loop():
    scanner = FakeScanner(error=RuntimeError("transient"))
    sink = CountingSink()
    service = FleetCopilotService(make_config(interval=0.02), scanner, sink)

    async def stopper():
        while scanner.calls < 3:
            await asyncio.sleep(0.01)
        service.request_stop()

    async def run():
        await asyncio.wait_for(
            asyncio.gather(service.run_forever(), stopper()),
            timeout=5.0,
        )

    asyncio.run(run())
    assert scanner.calls >= 3
    assert sink.calls == 0
    assert service.last_result is None
