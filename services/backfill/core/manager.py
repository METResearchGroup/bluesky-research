"""Manager class for running PDS backfill sync across multiple DIDs and PDS
endpoints."""

import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
import time
from typing import Iterable

from services.backfill.config.schema import BackfillConfigSchema
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.telemetry.prometheus.server import start_metrics_server
from services.backfill.core.worker import (
    get_previously_processed_dids,
    PDSEndpointWorker,
)

logger = get_logger(__name__)

# start Prometheus server for tracking.
start_metrics_server(port=8000)


class PdsEndpointManager:
    """Manages threaded PDS endpoint backfills."""

    def __init__(self, config: BackfillConfigSchema, plc_docs: list[dict]):
        self.config = config
        print("Config for PDS backfill:")
        pprint(self.config.model_dump())
        self.plc_docs = plc_docs
        self.pds_endpoint_to_dids_map = self.generate_pds_endpoint_to_dids_map()

        self.max_threads = 6
        self.max_workers_per_thread = 4
        self.max_endpoints_to_sync = 10  # set arbitrarily for now.

        self.valid_endpoints = self.validate_pds_endpoints_to_sync()
        logger.info(f"[PdsEndpointManager]: Valid endpoints: {self.valid_endpoints}")

        self.completed_pds_endpoint_backfills = (
            self.calculate_completed_pds_endpoint_backfills()
        )
        logger.info(
            f"[PdsEndpointManager]: Completed PDS endpoint backfills: {self.completed_pds_endpoint_backfills}"
        )

        self.sorted_endpoints = self.sort_endpoints_by_num_dids()
        logger.info(
            f"[PdsEndpointManager]: Sorted endpoints by number of DIDs: {self.sorted_endpoints}"
        )

        self.pds_endpoints_to_sync = self.filter_pds_endpoints_to_sync()
        logger.info(
            f"[PdsEndpointManager]: PDS endpoints to sync: {self.pds_endpoints_to_sync}"
        )
        logger.info(
            f"[PdsEndpointManager]: Total PDS endpoints to sync: {len(self.pds_endpoints_to_sync)}"
        )

        self.pds_endpoint_to_did_counts_map = {
            pds_endpoint: len(dids)
            for pds_endpoint, dids in self.pds_endpoint_to_dids_map.items()
            if pds_endpoint in self.pds_endpoints_to_sync
        }
        logger.info(
            f"[PdsEndpointManager]: PDS endpoint to DID counts map: {self.pds_endpoint_to_did_counts_map}"
        )
        logger.info(
            f"[PdsEndpointManager]: Total DIDs to sync: {sum(self.pds_endpoint_to_did_counts_map.values())}"
        )

    def generate_pds_endpoint_to_dids_map(self) -> dict[str, Iterable[str]]:
        """Generates the PLC endpoint to DIDs map."""
        pds_endpoint_to_dids_map: dict[str, set[str]] = {}
        for plc_doc in self.plc_docs:
            did = plc_doc["did"]
            service_endpoint = plc_doc["pds_service_endpoint"]
            if service_endpoint not in pds_endpoint_to_dids_map:
                pds_endpoint_to_dids_map[service_endpoint] = set()
            pds_endpoint_to_dids_map[service_endpoint].add(did)
        return pds_endpoint_to_dids_map

    def sort_endpoints_by_num_dids(self) -> list[str]:
        """Sorts the endpoints by number of DIDs in descending order."""
        sorted_endpoints = sorted(
            self.pds_endpoint_to_dids_map.items(),
            key=lambda item: len(item[1]),
            reverse=True,
        )
        return [endpoint for endpoint, _ in sorted_endpoints]

    def calculate_completed_pds_endpoint_backfills(self) -> list[str]:
        """Calculates the PDS endpoints that have already been backfilled."""
        completed_pds_endpoint_backfills = ["invalid_doc"]
        for pds_endpoint in self.pds_endpoint_to_dids_map.keys():
            if self.check_if_pds_endpoint_backfill_completed(
                pds_endpoint=pds_endpoint,
                expected_total=len(self.pds_endpoint_to_dids_map[pds_endpoint]),
            ):
                completed_pds_endpoint_backfills.append(pds_endpoint)
        return completed_pds_endpoint_backfills

    def check_if_pds_endpoint_backfill_completed(
        self, pds_endpoint: str, expected_total: int
    ) -> bool:
        """Checks if the PDS endpoint backfill is completed."""
        previously_processed_dids: set[str] = get_previously_processed_dids(
            pds_endpoint=pds_endpoint,
            min_timestamp=self.config.sync_storage.min_timestamp,
        )
        logger.info(
            f"(PDS endpoint: {pds_endpoint}): Finished backfilling {len(previously_processed_dids)}/{expected_total} DIDs."
        )
        return len(previously_processed_dids) == expected_total

    def validate_pds_endpoints_to_sync(self) -> list[str]:
        """Validates the PDS endpoints to sync."""
        total_valid = 0
        total_invalid = 0
        valid_endpoints = []
        for pds_endpoint in self.pds_endpoint_to_dids_map.keys():
            if "bsky.network" in pds_endpoint:
                valid_endpoints.append(pds_endpoint)
                total_valid += 1
            else:
                total_invalid += 1
        logger.info(
            f"[PdsEndpointManager]: Total valid endpoints: {total_valid}\nTotal invalid endpoints: {total_invalid}"
        )
        return valid_endpoints

    def get_completed_pds_endpoint_backfills(self) -> list[str]:
        """Gets the PDS endpoints that have already been backfilled."""
        return self.completed_pds_endpoint_backfills

    def filter_pds_endpoints_to_sync(self) -> list[str]:
        """Filters the PDS endpoints to sync

        1. Filter out endpoints that have already been synced.
        2. Take the top n endpoints as defined by max_endpoints_to_sync.
        3. Return the list of endpoints to sync, in the order to sync them.
        """
        filtered_endpoints = [
            endpoint
            for endpoint in self.sorted_endpoints
            if endpoint not in self.completed_pds_endpoint_backfills
            and endpoint in self.valid_endpoints
        ]

        # TODO: just sync all endpoints for now.
        # filtered_endpoints = filtered_endpoints[: self.max_endpoints_to_sync]

        logger.info(f"Filtered down to {len(filtered_endpoints)} PDS endpoints to sync")

        return filtered_endpoints

    async def run_pds_backfills(self) -> None:
        """Runs backfills for a list of DIDs.

        Spins up one instance of `PDSEndpointWorker` for each PDS endpoint, which
        manages all HTTP requests, DB writes, and rate limiting for that PDS endpoint.

        HTTP requests are managed by semaphores, DB writes are offloaded to a
        background task, and CPU-intensive work is offloaded to a thread pool
        shared across all worker instances of `PDSEndpointWorker`.
        """
        start_time = time.time()
        start_timestamp = generate_current_datetime_str()
        logger.info(
            f"[PDSEndpointManager]: Starting PDS backfills at {start_timestamp}..."
        )
        logger.info(
            f"Syncing {len(self.pds_endpoints_to_sync)} PDS endpoints: {self.pds_endpoints_to_sync}"
        )

        semaphore = asyncio.Semaphore(self.max_threads)

        async def run_single_pds_backfill(pds_endpoint: str, dids: list[str]) -> None:
            cpu_pool = ThreadPoolExecutor(max_workers=self.max_workers_per_thread)
            async with aiohttp.ClientSession() as session:
                logger.info(
                    f"Starting PDS backfill for {pds_endpoint} with {len(dids)} DIDs."
                )
                worker = PDSEndpointWorker(
                    pds_endpoint=pds_endpoint,
                    dids=dids,
                    session=session,
                    cpu_pool=cpu_pool,
                    config=self.config,
                )
                await worker.start()
                logger.info(
                    f"Worker started for {pds_endpoint}, waiting for completion..."
                )
                await worker.wait_done()
                await worker.shutdown()
                logger.info(f"Completed PDS backfill for {pds_endpoint}.")
            cpu_pool.shutdown(wait=True)

        async def limited_backfill(pds_endpoint: str, dids: list[str]) -> None:
            async with semaphore:
                await run_single_pds_backfill(pds_endpoint, dids)

        tasks = [
            limited_backfill(
                pds_endpoint=pds_endpoint,
                dids=self.pds_endpoint_to_dids_map[pds_endpoint],
            )
            for pds_endpoint in self.pds_endpoints_to_sync
        ]

        # Run them concurrently using asyncio
        await asyncio.gather(*tasks)

        end_time = time.time()
        end_timestamp = generate_current_datetime_str()
        total_time = end_time - start_time
        total_time_minutes = total_time / 60
        logger.info(
            f"[PDSEndpointManager]: PDS backfills completed at {end_timestamp}."
        )
        logger.info(
            f"[PDSEndpointManager]: Total time taken: {total_time_minutes} minutes"
        )

    def run_backfills(self) -> None:
        """Runs backfills for a list of DIDs."""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_pds_backfills())
        logger.info(
            f"PDS backfills completed for {len(self.pds_endpoints_to_sync)} endpoints and {len(self.plc_docs)} DIDs."
        )
