import asyncio
from pprint import pprint

from lib.log.logger import get_logger
from services.backfill.sync.backfill_endpoint_worker import (
    # PDSEndpointWorkerAsynchronous,
    # run_backfill_for_dids, parallel_request_test
    multithreaded_run_backfill_for_dids,
)
from services.backfill.sync.backfill_manager import (
    generate_pds_endpoint_to_dids_map,
    load_did_to_pds_endpoint_map,
)

logger = get_logger(__name__)


def main():
    max_pds_endpoints_to_sync = 10

    did_to_pds_endpoint_map: dict[str, str] = load_did_to_pds_endpoint_map()
    pds_endpoint_to_dids_map: dict[str, list[str]] = generate_pds_endpoint_to_dids_map(
        did_to_pds_endpoint_map=did_to_pds_endpoint_map,
    )

    pds_endpoint_to_did_count = {
        endpoint: len(dids) for endpoint, dids in pds_endpoint_to_dids_map.items()
    }

    sorted_endpoints = sorted(
        pds_endpoint_to_did_count.items(), key=lambda item: item[1], reverse=True
    )
    # We only want to sync the top N PDS endpoints.
    sorted_endpoints = sorted_endpoints[:max_pds_endpoints_to_sync]
    sorted_endpoints = [endpoint for endpoint, _ in sorted_endpoints]
    sorted_dict = {key: pds_endpoint_to_did_count[key] for key in sorted_endpoints}
    logger.info("Sorted PDS endpoints by number of DIDs (descending order):")
    pprint(sorted_dict)
    pds_endpoint_to_dids_map = {
        endpoint: dids
        for endpoint, dids in pds_endpoint_to_dids_map.items()
        if endpoint in sorted_endpoints
    }
    logger.info(f"Only sorting the top {max_pds_endpoints_to_sync} PDS endpoints.")

    for pds_endpoint, dids in pds_endpoint_to_dids_map.items():
        if pds_endpoint != "https://morel.us-east.host.bsky.network":
            continue
        loop = asyncio.get_event_loop()
        # endpoint_worker = PDSEndpointWorkerAsynchronous(
        #     pds_endpoint=pds_endpoint,
        #     dids=dids,
        # )
        # loop.run_until_complete(endpoint_worker.run())

        # NOTE: confirmed that this works.
        # loop.run_until_complete(parallel_request_test())
        # loop.run_until_complete(
        #     run_backfill_for_dids(
        #         pds_endpoint=pds_endpoint,
        #         dids=dids,
        #     )
        # )
        pds_endpoint = "https://bsky.social"

        # two sets of DIDS that I know are hosted in different PDS endpoints,
        # to see if the rate limit header is different for each set.
        dids_1 = [dids[0] for _ in range(100)]
        dids_2 = ["did:plc:w5mjarupsl6ihdrzwgnzdh4y" for _ in range(100)]

        # loop.run_until_complete(
        #     multithreaded_run_backfill_for_dids(
        #         pds_endpoint=pds_endpoint,
        #         dids=dids,
        #     )
        # )
        loop.run_until_complete(
            multithreaded_run_backfill_for_dids(
                pds_endpoint=pds_endpoint,
                dids=dids_1,
            )
        )
        loop.run_until_complete(
            multithreaded_run_backfill_for_dids(
                pds_endpoint=pds_endpoint,
                dids=dids_2,
            )
        )

        # from services.backfill.sync.atp_agent import AtpAgent
        # atp_agent = AtpAgent()
        # # atp_agent = AtpAgent(service=pds_endpoint)

        # responses = []

        # def run_list_records(n: int):
        #     for _ in range(n):
        #         response = atp_agent.list_records(
        #             repo="did:plc:4rlh46czb2ix4azam3cfyzys",
        #             collection="app.bsky.feed.post",
        #             limit=100,
        #         )
        #         responses.append(response)

        # # TODO: check rate limits.
        # run_list_records(100)

        # # TODO: check rate limits.
        # breakpoint()

    logger.info("Async PDS backfills completed.")


if __name__ == "__main__":
    main()
