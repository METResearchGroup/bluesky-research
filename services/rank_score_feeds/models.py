from dataclasses import dataclass
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field

from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)
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


# Data carrier dataclasses for orchestrator (Phase 1)


@dataclass
class LoadedData:
    """Container for all loaded input data."""

    posts_df: pd.DataFrame
    posts_models: list[ConsolidatedEnrichedPostModel]
    user_to_social_network_map: dict[str, list[str]]
    superposter_dids: set[str]
    previous_feeds: dict[str, set[str]]  # user handle -> set of URIs
    study_users: list[UserToBlueskyProfileModel]


@dataclass
class ScoredPosts:
    """Container for posts with scores."""

    posts_df: pd.DataFrame  # includes engagement_score and treatment_score columns
    new_post_uris: list[str]  # URIs of posts that were newly scored


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
