import asyncio
import logging

from .config import settings
from .collector_worker import CollectorFleetWorker


def configure_logging():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def main():
    worker = CollectorFleetWorker()
    await worker.run_forever()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
