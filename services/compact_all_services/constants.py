"""Constants for compact_all_services."""

import os

MAP_SERVICE_TO_METADATA = {
    # user_session_logs shouldn't be deduped, each row already is
    # unique. Just need to compress.
    "user_session_logs": {
        "s3_prefix": "user_session_logs",
        "glue_table_name": "user_session_logs",
        "primary_key": "",
        "timestamp_field": "",
        "skip_deduping": True,
    },
    "feed_analytics": {
        "s3_prefix": "feed_analytics",
        "glue_table_name": "feed_generation_session_analytics",
        "primary_key": "",
        "timestamp_field": "",
        "skip_deduping": True,
    },
    "post_scores": {
        "s3_prefix": "post_scores",
        "glue_table_name": "post_scores",
        "primary_key": "uri",
        "timestamp_field": "scored_timestamp",
        "skip_deduping": False,
    },
    "consolidated_enriched_post_records": {
        "s3_prefix": "consolidated_enriched_post_records",
        "glue_table_name": "consolidated_enriched_post_records",
        "primary_key": "uri",
        "timestamp_field": "consolidation_timestamp",
        "skip_deduping": False,
    },
    "ml_inference_perspective_api": {
        "s3_prefix": "ml_inference_perspective_api",
        "glue_table_name": "ml_inference_perspective_api",
        "primary_key": "uri",
        "timestamp_field": "label_timestamp",
        "skip_deduping": False,
    },
    "ml_inference_sociopolitical": {
        "s3_prefix": "ml_inference_sociopolitical",
        "glue_table_name": "ml_inference_sociopolitical",
        "primary_key": "uri",
        "timestamp_field": "label_timestamp",
        "skip_deduping": False,
    },
    "in_network_user_activity": {
        "s3_prefix": os.path.join("in_network_user_activity", "create", "post"),
        "glue_table_name": "in_network_firehose_sync_posts",
        "primary_key": "uri",
        "timestamp_field": "synctimestamp",
        "skip_deduping": False,
    },
    # each row is a unique relationship, so no deduping required.
    "scraped_user_social_network": {
        "s3_prefix": "scraped-user-social-network",
        "glue_table_name": "user_social_networks",
        "primary_key": "",
        "timestamp_field": "",
        "skip_deduping": True,
    },
}
