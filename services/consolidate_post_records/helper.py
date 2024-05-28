"""Consolidates different types of post records into a single format."""
from typing import Union

from lib.constants import current_datetime_str
from lib.db.bluesky_models.transformations import (
    TransformedRecordWithAuthorModel, TransformedFeedViewPostModel
)
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.consolidate_post_records.models import (
    ConsolidatedMetrics, ConsolidatedPostRecordModel,
    ConsolidatedPostRecordMetadataModel
)

logger = get_logger(__file__)


def consolidate_firehose_post(post: TransformedRecordWithAuthorModel) -> ConsolidatedPostRecordModel:  # noqa
    """Transforms the firehose posts into the consolidated format."""
    metadata_dict = {
        "synctimestamp": post.metadata.synctimestamp,
        "url": post.metadata.url,
        "source": "firehose",
        "processed_timestamp": current_datetime_str
    }
    metadata: ConsolidatedPostRecordMetadataModel = (
        ConsolidatedPostRecordMetadataModel(**metadata_dict)
    )
    metrics_dict = {
        "like_count": None, "reply_count": None, "repost_count": None
    }
    metrics: ConsolidatedMetrics = ConsolidatedMetrics(**metrics_dict)
    res = {
        "uri": post.uri,
        "cid": post.cid,
        "indexed_at": post.indexed_at,
        "author": post.author,
        "metadata": metadata,
        "record": post.record,
        "metrics": metrics
    }
    return ConsolidatedPostRecordModel(**res)


def consolidate_feedview_post(post: TransformedFeedViewPostModel) -> ConsolidatedPostRecordModel:  # noqa
    """Transforms the feed view posts into the consolidated format."""
    metadata_dict = {
        "synctimestamp": post.metadata.synctimestamp,
        "url": post.metadata.url,
        "source": "most_liked",
        "processed_timestamp": current_datetime_str
    }
    metadata: ConsolidatedPostRecordMetadataModel = (
        ConsolidatedPostRecordMetadataModel(**metadata_dict)
    )
    metrics_dict = {
        "like_count": post.like_count,
        "reply_count": post.reply_count,
        "repost_count": post.repost_count
    }
    metrics: ConsolidatedMetrics = ConsolidatedMetrics(**metrics_dict)
    res = {
        "uri": post.uri,
        "cid": post.cid,
        "indexed_at": post.indexed_at,
        "author": post.author,
        "metadata": metadata,
        "record": post.record,
        "metrics": metrics
    }
    return ConsolidatedPostRecordModel(**res)


def consolidate_post_record(
    post: Union[TransformedFeedViewPostModel, TransformedRecordWithAuthorModel]
) -> ConsolidatedPostRecordModel:
    if isinstance(post, TransformedFeedViewPostModel):
        return consolidate_feedview_post(post)
    elif isinstance(post, TransformedRecordWithAuthorModel):
        return consolidate_firehose_post(post)
    else:
        raise ValueError(f"Unknown post type: {type(post)}")


@track_performance
def consolidate_post_records(
    posts: list[
        Union[TransformedFeedViewPostModel, TransformedRecordWithAuthorModel]
    ]
) -> list[ConsolidatedPostRecordModel]:
    logger.info(f"Consolidated the formats of {len(posts)} posts...")
    res = [consolidate_post_record(post) for post in posts]
    logger.info(f"Finished consolidating the formats of {len(posts)} posts.")
    return res


if __name__ == "__main__":
    pass
