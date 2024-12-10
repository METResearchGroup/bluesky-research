"""Helper functions for interacting with data storage."""

import pandas as pd

quest_data_base_directory = "/projects/p32375/bluesky_research_data/"
local_data_base_directory = "/Users/mark/Documents/work/bluesky_research_data"
data_base_directory = quest_data_base_directory

list_service_folders = [
    # sync services
    "sync/most_liked",
    "scraped_user_social_network",
    "in_network_user_activity",
    "study_user_activity/create/like",
    "study_user_activity/create/like_on_user_post",
    "study_user_activity/create/post",
    "study_user_activity/create/reply_to_user_post",
    # preprocessing
    "preprocessed_posts/firehose",
    "preprocessed_posts/most_liked",
    # integrations
    "daily_superposters",
    "ml_inference_ime",
    "ml_inference_perspective_api/firehose",
    "ml_inference_perspective_api/most_liked",
    "ml_inference_sociopolitical/firehose",
    "ml_inference_sociopolitical/most_liked",
    # consolidation
    "consolidated_enriched_post_records",
    # feeds
    "feed_analytics",
    "post_scores",
]


# TODO: refactor, I think i can get this from manage_local_data.py
def get_service_data_from_partition_date(
    service_folder: str,
    partition_date: str,
) -> pd.DataFrame:
    """Get the data for a given service and partition date."""
    pass
