"""Constants for service."""

from lib.constants import root_local_data_directory

import os

MAP_SERVICE_TO_METADATA = {
    # user_session_logs shouldn't be deduped, each row already is
    # unique. Just need to compress.
    "user_session_logs": {
        "local_prefix": os.path.join(root_local_data_directory, "user_session_logs"),
        "s3_prefix": "user_session_logs",
        "glue_table_name": "user_session_logs",
        "primary_key": "",
        "timestamp_field": "timestamp",
        "skip_deduping": True,
        "pydantic_model": "",
    },
    "feed_analytics": {
        "local_prefix": os.path.join(root_local_data_directory, "feed_analytics"),
        "s3_prefix": "feed_analytics",
        "glue_table_name": "feed_generation_session_analytics",
        "primary_key": "",
        "timestamp_field": "session_timestamp",
        "skip_deduping": True,
        "pydantic_model": "",
    },
    # TODO: update this.
    # TODO: there will also be a complex dtype.
    # "daily_superposters": {
    #     "local_prefix": os.path.join(root_local_data_directory, "daily_superposters"),
    #     "s3_prefix": "daily_superposters",
    #     "glue_table_name": "daily_superposters",
    #     "primary_key": "",
    #     "timestamp_field": "session_timestamp",
    #     "skip_deduping": True,
    #     "pydantic_model": "",
    # },
    "post_scores": {
        "local_prefix": os.path.join(root_local_data_directory, "post_scores"),
        "s3_prefix": "post_scores",
        "glue_table_name": "post_scores",
        "primary_key": "uri",
        "timestamp_field": "scored_timestamp",
        "skip_deduping": False,
        "pydantic_model": "",
    },
    "consolidated_enriched_post_records": {
        "local_prefix": os.path.join(
            root_local_data_directory, "consolidated_enriched_post_records"
        ),
        "s3_prefix": "consolidated_enriched_post_records",
        "glue_table_name": "consolidated_enriched_post_records",
        "primary_key": "uri",
        "timestamp_field": "consolidation_timestamp",
        "skip_deduping": False,
        "pydantic_model": "",
        # https://pandas.pydata.org/pandas-docs/stable/user_guide/basics.html#basics-dtypes
        # https://stackoverflow.com/questions/60377531/pandas-valueerror-integer-column-has-na-values-in-column-2
        "dtypes_map": {
            "labels": "object",
            "like_count": "Int64",
            "reply_count": "Int64",
            "repost_count": "Int64",
            "filtered_by_func": "object",
            "sociopolitical_reason": "object",
        },
    },
    "ml_inference_perspective_api": {
        "local_prefix": os.path.join(
            root_local_data_directory, "ml_inference_perspective_api"
        ),
        "s3_prefix": "ml_inference_perspective_api",
        "glue_table_name": "ml_inference_perspective_api",
        "primary_key": "uri",
        "timestamp_field": "label_timestamp",
        "skip_deduping": False,
        "pydantic_model": "",
    },
    "ml_inference_sociopolitical": {
        "local_prefix": os.path.join(
            root_local_data_directory, "ml_inference_sociopolitical"
        ),
        "s3_prefix": "ml_inference_sociopolitical",
        "glue_table_name": "ml_inference_sociopolitical",
        "primary_key": "uri",
        "timestamp_field": "label_timestamp",
        "skip_deduping": False,
        "pydantic_model": "",
    },
    "in_network_user_activity": {
        "local_prefix": os.path.join(
            root_local_data_directory, "in_network_user_activity"
        ),
        "s3_prefix": os.path.join("in_network_user_activity", "create", "post"),
        "glue_table_name": "in_network_firehose_sync_posts",
        "primary_key": "uri",
        "timestamp_field": "synctimestamp",
        "skip_deduping": False,
        "pydantic_model": "",
        "dtypes_map": {
            "uri": "object",
            "cid": "object",
            "indexed_at": "object",
            "author_did": "object",
            "author_handle": "object",
            "author_avatar": "object",
            "author_display_name": "object",
            "created_at": "object",
            "text": "object",
            "embed": "object",
            "entities": "object",
            "facets": "object",
            "labels": "object",
            "langs": "object",
            "reply_parent": "object",
            "reply_root": "object",
            "tags": "object",
            "synctimestamp": "object",
            "url": "object",
            "source": "object",
            "like_count": "Int64",
            "reply_count": "Int64",
            "repost_count": "Int64",
        },
    },
    # each row is a unique relationship, so no deduping required.
    "scraped_user_social_network": {
        "local_prefix": os.path.join(
            root_local_data_directory, "scraped_user_social_network"
        ),
        "s3_prefix": "scraped-user-social-network",
        "glue_table_name": "user_social_networks",
        "primary_key": "",
        "timestamp_field": "insert_timestamp",
        "skip_deduping": True,
        "pydantic_model": "",
        # string is more memory-efficient for actual strings.
        "dtypes_map": {
            "follow_handle": "string",
            "follow_url": "string",
            "follow_did": "string",
            "follower_handle": "string",
            "follower_url": "string",
            "follower_did": "string",
            "insert_timestamp": "string",
            "relationship_to_study_user": "string",
        },
    },
}