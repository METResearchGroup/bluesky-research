from dataclasses import dataclass
from typing import Optional, Set

import pandas as pd
from pydantic import BaseModel, Field

from services.participant_data.models import UserToBlueskyProfileModel


class ScoredPostModel(BaseModel):
    """Model for representing a scored post."""

    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the record.")
    engagement_score: Optional[float] = Field(
        default=None, description="Engagement score of the post."
    )
    treatment_score: Optional[float] = Field(
        default=None, description="Treatment score of the post."
    )
    source: str = Field(
        ..., description="The source of the post (e.g. 'firehose', 'most_liked')."
    )
    scored_timestamp: str = Field(
        ..., description="Timestamp when the post was scored."
    )  # noqa


class CustomFeedPost(BaseModel):
    """Post in a custom feed."""

    item: str = Field(..., description="The post identifier (URI).")
    is_in_network: bool = Field(
        ..., description="Whether the post is in-network or not."
    )


class CustomFeedModel(BaseModel):
    """Model for representing a custom feed for a user."""

    # keeping 'user' field for backwards compatibility.
    feed_id: str = Field(..., description="The feed identifier (DID::timestamp).")
    user: str = Field(..., description="The user identifier (DID).")
    bluesky_handle: str = Field(..., description="The Bluesky handle of the user.")  # noqa
    bluesky_user_did: str = Field(
        ..., description="The Bluesky user DID. Same as `user`."
    )  # noqa
    condition: str = Field(..., description="The condition of the study user and feed.")  # noqa
    feed_statistics: str = Field(
        ..., description="The JSON-dumped statistics of the feed."
    )  # noqa
    feed: list[CustomFeedPost] = Field(
        ...,
        description="List of posts in the feed along with if the post is in-network or not.",  # noqa
    )
    feed_generation_timestamp: str = Field(
        ..., description="Timestamp when the feed was generated."
    )


class LatestFeeds(BaseModel):
    """Model representing the latest feeds per user.

    Maps Bluesky user handles to sets of post URIs from their latest feed.
    Provides dict-like interface for backward compatibility.
    """

    feeds: dict[str, set[str]] = Field(
        ...,
        description="Mapping of Bluesky user handles to sets of post URIs in their latest feed.",
    )

    def get(self, handle: str, default: Set[str] | None = None) -> Set[str]:
        """Get the feed URIs for a specific user handle.

        Args:
            handle: The Bluesky user handle.
            default: Default value to return if handle not found. Defaults to empty set.

        Returns:
            Set of post URIs for the user, or default value if handle not found.
        """
        if default is None:
            default = set()
        return self.feeds.get(handle, default)

    def __getitem__(self, handle: str) -> Set[str]:
        """Allow dict-like access: latest_feeds[handle]"""
        return self.feeds[handle]

    def keys(self):
        """Get all user handles."""
        return self.feeds.keys()

    def values(self):
        """Get all feed URI sets."""
        return self.feeds.values()

    def items(self):
        """Get all (handle, uri_set) pairs."""
        return self.feeds.items()

    def __contains__(self, handle: str) -> bool:
        """Check if a handle exists: handle in latest_feeds"""
        return handle in self.feeds

    def __len__(self) -> int:
        """Get the number of feeds."""
        return len(self.feeds)


# Data carrier dataclasses for orchestrator (Phase 1)


class FeedInputData(BaseModel):
    """Input data required for feed generation.

    Contains all the data sources needed to generate ranked feeds:
    - Enriched posts from the consolidation service
    - User social network mappings
    - Superposter DIDs for filtering
    """

    model_config = {"arbitrary_types_allowed": True}

    consolidate_enrichment_integrations: pd.DataFrame
    scraped_user_social_network: dict[str, list[str]]
    superposters: set[str]


class RawFeedData(BaseModel):
    """Raw data loaded from all sources before transformation.

    Contains the unprocessed data from all services needed for feed generation.
    """

    study_users: list[UserToBlueskyProfileModel]
    feed_input_data: FeedInputData
    latest_feeds: LatestFeeds


class LoadedData(BaseModel):
    """Container for all loaded input data."""

    model_config = {"arbitrary_types_allowed": True}

    posts_df: pd.DataFrame
    user_to_social_network_map: dict[str, list[str]]
    superposter_dids: set[str]
    previous_feeds: LatestFeeds  # user handle -> set of URIs
    study_users: list[UserToBlueskyProfileModel]


class PostScoreByAlgorithm(BaseModel):
    uri: str = Field(..., description="The URI of the post.")
    engagement_score: float = Field(
        ..., description="The engagement score of the post."
    )
    treatment_score: float = Field(..., description="The treatment score of the post.")


@dataclass
class PostsSplitByScoringStatus:
    """Posts split by whether they've been scored."""

    already_scored: pd.DataFrame
    not_scored_yet: pd.DataFrame


@dataclass
class PostPools:
    """Container for the three post pools used for ranking."""

    reverse_chronological: pd.DataFrame
    engagement: pd.DataFrame
    treatment: pd.DataFrame


@dataclass
class UserFeedResult:
    """Result for a single user's feed."""

    user_did: str
    bluesky_handle: str
    condition: str
    feed: list[CustomFeedPost]
    feed_statistics: str  # JSON string


@dataclass
class RunResult:
    """Final result of a feed generation run."""

    user_feeds: dict[str, UserFeedResult]  # user_did -> feed result
    default_feed: UserFeedResult
    analytics: dict
    timestamp: str
