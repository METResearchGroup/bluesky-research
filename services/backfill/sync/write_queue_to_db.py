"""One-off script to write a queue DB to the database and export metadata (if relevant)."""

import asyncio

from services.backfill.sync.backfill_endpoint_worker import PDSEndpointWorker


async def write_pds_queue_to_db(pds_endpoint: str) -> None:
    """Writes a PDS queue to DB and then exports metadata."""
    print(f"Writing PDS queue to DB for {pds_endpoint}...")
    worker = PDSEndpointWorker(
        pds_endpoint=pds_endpoint,
        dids=[],
        session=None,
        cpu_pool=None,
    )
    user_to_total_per_record_type_map = await worker.persist_to_db()
    print("Finished persisting to DB. Now exporting metadata...")
    print(f"Writing backfill metadata to DB for {pds_endpoint}...")
    worker.write_backfill_metadata_to_db(
        user_to_total_per_record_type_map=user_to_total_per_record_type_map
    )
    print(f"Completed writing PDS queue to DB for {pds_endpoint}.")


if __name__ == "__main__":
    pds_endpoint = "https://meadow.us-east.host.bsky.network"
    asyncio.run(write_pds_queue_to_db(pds_endpoint=pds_endpoint))
