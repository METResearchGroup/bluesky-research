from dataclasses import dataclass
from enum import Enum
from typing import Optional, Set, TypeAlias

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


class StoredFeedModel(BaseModel):
    """Model for representing the version of the feed that is persisted to storage.
    
    This is the version of the feed loaded from storage and served to the user
    via our FastAPI app.
    """

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


class FeedWithMetadata(BaseModel):
    """Model for representing a feed with metadata."""

    feed: list[CustomFeedPost]
    bluesky_handle: str
    user_did: str
    condition: str
    feed_statistics: str

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
class CandidatePostPools:
    """Container for the three post pools used for ranking."""

    reverse_chronological: pd.DataFrame
    engagement: pd.DataFrame
    treatment: pd.DataFrame



# TODO: delete. Replaced with FeedWithMetadata. Just need to make
# sure that it is 100% compatible across all call sites.
# @dataclass
# class UserFeedResult:
#     """Result for a single user's feed."""

#     user_did: str
#     bluesky_handle: str
#     condition: str
#     feed: list[CustomFeedPost]
#     feed_statistics: str  # JSON string


# TODO: rename to FeedGenerationResult (if I even need it still?)
# @dataclass
# class RunResult:
#     """Final result of a feed generation run."""

#     user_feeds: dict[str, UserFeedResult]  # user_did -> feed result
#     default_feed: UserFeedResult
#     analytics: dict
#     timestamp: str


class FreshnessScoreFunction(str, Enum):
    """Enum for the freshness score function."""

    LINEAR = "linear"
    EXPONENTIAL = "exponential"

    def __str__(self) -> str:
        """Return the string representation of the freshness score function."""
        return self.value

# add a type alias for the user in network posts map for readability.
UserInNetworkPostsMap: TypeAlias = dict[str, list[str]]  # user_did -> list[post_uri]


class FeedGenerationSessionAnalytics(BaseModel):
    """Model for feed generation session analytics.
    
    Contains aggregated statistics for a single feed generation session,
    including counts, proportions, and overlap metrics between different feed conditions.
    """

    total_feeds: int = Field(..., description="Total number of feeds generated in the session.")
    total_posts: int = Field(..., description="Total number of posts across all feeds.")
    total_in_network_posts: int = Field(
        ..., description="Total number of in-network posts across all feeds."
    )
    total_in_network_posts_prop: float = Field(
        ...,
        description="Proportion of posts that are in-network (rounded to 2 decimal places).",
        ge=0.0,
        le=1.0,
    )
    total_unique_engagement_uris: int = Field(
        ..., description="Total number of unique post URIs in engagement condition feeds."
    )
    total_unique_treatment_uris: int = Field(
        ...,
        description="Total number of unique post URIs in representative_diversification condition feeds.",
    )
    prop_overlap_treatment_uris_in_engagement_uris: float = Field(
        ...,
        description=(
            "Proportion of treatment URIs that overlap with engagement URIs "
            "(rounded to 3 decimal places)."
        ),
        ge=0.0,
        le=1.0,
    )
    prop_overlap_engagement_uris_in_treatment_uris: float = Field(
        ...,
        description=(
            "Proportion of engagement URIs that overlap with treatment URIs "
            "(rounded to 3 decimal places)."
        ),
        ge=0.0,
        le=1.0,
    )
    total_feeds_per_condition: dict[str, int] = Field(
        ...,
        description=(
            "Mapping of condition names to the number of feeds generated for each condition. "
            "Keys are: 'representative_diversification', 'engagement', 'reverse_chronological'."
        ),
    )
    session_timestamp: str = Field(
        ..., description="Timestamp when the feed generation session occurred."
    )
