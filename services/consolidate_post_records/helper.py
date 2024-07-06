"""Consolidates different types of post records into a single format."""
from typing import Union

from lib.constants import current_datetime_str
from lib.db.bluesky_models.transformations import (
    TransformedProfileViewBasicModel, TransformedRecordWithAuthorModel,
    TransformedFeedViewPostModel
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
    # firehose posts don't have the author hydrated, so all we have is the DID.
    author_dict = {
        "did": post.author,
        "handle": None,
        "avatar": None,
        "display_name": None
    }
    author = TransformedProfileViewBasicModel(**author_dict)
    metrics: ConsolidatedMetrics = ConsolidatedMetrics(**metrics_dict)
    res = {
        "uri": post.uri,
        "cid": post.cid,
        "indexed_at": None,  # not available in the firehose post.
        "author_did": author.did,
        "author_handle": author.handle,
        "author_avatar": author.avatar,
        "author_display_name": author.display_name,
        "created_at": post.record.created_at,
        "text": post.record.text,
        "embed": post.record.embed,
        "entities": post.record.entities,
        "facets": post.record.facets,
        "labels": post.record.labels,
        "langs": post.record.langs,
        "reply_parent": post.record.reply_parent,
        "reply_root": post.record.reply_root,
        "tags": post.record.tags,
        "synctimestamp": metadata.synctimestamp,
        "url": metadata.url,
        "source": metadata.source,
        "like_count": metrics.like_count,
        "reply_count": metrics.reply_count,
        "repost_count": metrics.repost_count
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
        "author_did": post.author.did,
        "author_handle": post.author.handle,
        "author_avatar": post.author.avatar,
        "author_display_name": post.author.display_name,
        "created_at": post.record.created_at,
        "text": post.record.text,
        "embed": post.record.embed,
        "entities": post.record.entities,
        "facets": post.record.facets,
        "labels": post.record.labels,
        "langs": post.record.langs,
        "reply_parent": post.record.reply_parent,
        "reply_root": post.record.reply_root,
        "tags": post.record.tags,
        "synctimestamp": metadata.synctimestamp,
        "url": metadata.url,
        "source": metadata.source,
        "like_count": metrics.like_count,
        "reply_count": metrics.reply_count,
        "repost_count": metrics.repost_count
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
