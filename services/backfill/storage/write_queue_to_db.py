"""One-off script to write a queue DB to the database and export metadata (if relevant)."""

import asyncio

from services.backfill.core.worker import PDSEndpointWorker


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
    if user_to_total_per_record_type_map:
        worker.write_backfill_metadata_to_db(
            user_to_total_per_record_type_map=user_to_total_per_record_type_map
        )
    else:
        print(f"No metadata to export for {pds_endpoint}.")
    worker.output_results_queue.delete_queue()
    worker.output_deadletter_queue.delete_queue()
    print(f"Completed writing PDS queue to DB for {pds_endpoint}.")


if __name__ == "__main__":
    # pds_endpoint = "https://meadow.us-east.host.bsky.network"
    # pds_endpoint = "https://shiitake.us-east.host.bsky.network"
    # pds_endpoint = "https://lionsmane.us-east.host.bsky.network"
    # pds_endpoint = "https://porcini.us-east.host.bsky.network"
    # pds_endpoint = "https://amanita.us-east.host.bsky.network"
    # pds_endpoint = "https://inkcap.us-east.host.bsky.network"
    # pds_endpoint = "https://oyster.us-east.host.bsky.network"
    # pds_endpoint = "https://inkcap.us-east.host.bsky.network"
    # pds_endpoint = "https://porcini.us-east.host.bsky.network"
    # pds_endpoint = "https://shimeji.us-east.host.bsky.network"
    # pds_endpoint = "https://puffball.us-east.host.bsky.network"
    # pds_endpoint = "https://chanterelle.us-east.host.bsky.network"
    # pds_endpoint = "https://splitgill.us-east.host.bsky.network"
    # pds_endpoint = "https://inkcap.us-east.host.bsky.network"
    # pds_endpoint = "https://panthercap.us-east.host.bsky.network"
    # pds_endpoint = "https://witchesbutter.us-east.host.bsky.network"
    # pds_endpoint = "https://splitgill.us-east.host.bsky.network"
    # pds_endpoint = "https://chanterelle.us-east.host.bsky.network"
    # pds_endpoint = "https://shimeji.us-east.host.bsky.network"
    pds_endpoint = "https://puffball.us-east.host.bsky.network"

    asyncio.run(write_pds_queue_to_db(pds_endpoint=pds_endpoint))
