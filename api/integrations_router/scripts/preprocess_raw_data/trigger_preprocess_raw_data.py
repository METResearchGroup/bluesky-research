"""Triggers the preprocess_raw_data pipeline."""

from api.integrations_router.models import IntegrationRequest
from api.integrations_router.run import run_integration_request


def main():
    """Triggers the preprocess_raw_data pipeline.

    We manually override the preprocessing timestamp to be the synctimestamp.
    Normally in production, we preprocess right after syncing, so after we
    preprocess, the post becomes available to downstream services. However,
    on backfills, we preprocess well after the post is synced, but we want
    the partitioning and storage to correctly store data from the backfill
    relative to when they would've been available had they been synced.

    Making this consistent across syncs and backfills means that we can
    have a reliable model for how the posts are partitioned; we don't want
    a day's posts to be a combination of both posts synced that day and
    posts that happened to be backfilled that day, we want posts to be
    partitioned into dates relative to when they would've been preprocessed
    in the normal pipeline.
    """
    request = {
        "service": "preprocess_raw_data",
        "payload": {},
        "metadata": {},
        "overwrite_preprocessing_timestamp": True,
        "new_timestamp_field": "synctimestamp",
    }
    request = IntegrationRequest(**request)
    run_integration_request(request)


if __name__ == "__main__":
    main()
