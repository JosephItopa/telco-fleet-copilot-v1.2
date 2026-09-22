import asyncio
import logging

from .config import settings
from .kubernetes_discovery import KubernetesDiscovery
from .metrics_api import KubernetesMetricsAPI
from .prometheus_scraper import PrometheusScraper
from .normalizer import MetricsNormalizer
from .kafka_producer import PartitionedMetricsProducer

LOG = logging.getLogger(__name__)


class CollectorFleetWorker:
    def __init__(self):
        self.discovery = KubernetesDiscovery(settings.kubernetes_namespace)
        self.metrics_api = KubernetesMetricsAPI()
        self.scraper = PrometheusScraper(
            timeout=settings.prometheus_timeout_seconds,
            max_concurrent=settings.max_concurrent_scrapes,
        )
        self.normalizer = MetricsNormalizer(settings.kubernetes_cluster_name)
        self.producer = PartitionedMetricsProducer()

    async def collect_once(self) -> None:
        targets = self.discovery.discover()

        async def collect_target(target):
            k8s_metrics = self.metrics_api.get_pod_metrics(
                target.namespace,
                target.pod_name,
            )
            prom_metrics = await self.scraper.scrape(target.scrape_url)

            return self.normalizer.build_event(
                target,
                k8s_metrics,
                prom_metrics,
            )

        events = await asyncio.gather(
            *(collect_target(target) for target in targets),
            return_exceptions=True,
        )

        published = 0
        for event in events:
            if isinstance(event, Exception):
                LOG.exception("Target collection failed", exc_info=event)
                continue

            if not event.metrics:
                continue

            self.producer.publish(event.as_dict())
            published += 1

        self.producer.flush()
        LOG.info("Published %d/%d application metric events", published, len(targets))

    async def run_forever(self) -> None:
        while True:
            try:
                await self.collect_once()
            except Exception:
                LOG.exception("Collector cycle failed")

            await asyncio.sleep(settings.collect_interval_seconds)
