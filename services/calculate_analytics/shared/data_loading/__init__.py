"""Shared data loading modules for analytics system.

This module provides unified interfaces for loading various types of data
used across the analytics system, eliminating code duplication and
ensuring consistent data handling patterns.
"""

from .posts import (
    load_filtered_preprocessed_posts,
    get_hydrated_posts_for_partition_date,
    load_posts_with_labels,
)
from .labels import (
    get_perspective_api_labels_for_posts,
    get_sociopolitical_labels_for_posts,
    get_ime_labels_for_posts,
    get_valence_labels_for_posts,
    load_all_labels_for_posts,
)
from .feeds import (
    get_feeds_for_partition_date,
    map_users_to_posts_used_in_feeds,
)
from .users import (
    load_user_demographic_info,
    get_user_condition_mapping,
)

__all__ = [
    # Posts
    "load_filtered_preprocessed_posts",
    "get_hydrated_posts_for_partition_date",
    "load_posts_with_labels",
    # Labels
    "get_perspective_api_labels_for_posts",
    "get_sociopolitical_labels_for_posts",
    "get_ime_labels_for_posts",
    "get_valence_labels_for_posts",
    "load_all_labels_for_posts",
    # Feeds
    "get_feeds_for_partition_date",
    "map_users_to_posts_used_in_feeds",
    # Users
    "load_user_demographic_info",
    "get_user_condition_mapping",
]
