"""Main script for the Bluesky firehose ingestion demo."""

import asyncio
import logging
import signal
from typing import Set
from rich.logging import RichHandler

from utils import FirehoseSubscriber, BatchWriter
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)

class FirehoseIngestion:
    """Main class for handling the firehose ingestion process."""

    def __init__(self) -> None:
        """Initialize the ingestion process."""
        self.subscriber = FirehoseSubscriber()
        self.writer = BatchWriter()
        self._stop_event = asyncio.Event()
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self._stop_event.set()

    async def run(self) -> None:
        """Run the ingestion process."""
        try:
            logger.info("Starting firehose ingestion...")
            logger.info(f"Watching for record types: {', '.join(settings.RECORD_TYPES)}")
            logger.info(f"Writing batches of {settings.BATCH_SIZE} records")
            logger.info(f"Storing data in: {settings.BASE_DIR}")

            async for record in self.subscriber.subscribe():
                if self._stop_event.is_set():
                    break
                await self.writer.add_record(record)

        except Exception as e:
            logger.error(f"Error in ingestion process: {e}")
        finally:
            logger.info("Flushing remaining records...")
            await self.writer.flush_all()
            logger.info("Ingestion process complete")

async def main() -> None:
    """Main entry point for the script."""
    ingestion = FirehoseIngestion()
    await ingestion.run()

if __name__ == "__main__":
    asyncio.run(main()) 