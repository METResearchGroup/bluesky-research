"""Pydantic models for the consolidated enrichment integrations."""

from pydantic import BaseModel, Field

from typing import Optional, Union
import typing_extensions as te

from lib.db.bluesky_models.embed import ProcessedEmbed


class ConsolidatedEnrichedPostModel(BaseModel):
    """Pydantic model for the consolidated enriched post."""

    # Fields from FilteredPreprocessedPostModel
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: Optional[str] = Field(
        default=None,
        description="The timestamp of when the post was indexed by Bluesky.",
    )
    author_did: str = Field(..., description="The DID of the user.")
    author_handle: Optional[str] = Field(
        default=None, description="The handle of the user."
    )
    author_avatar: Optional[str] = None
    author_display_name: Optional[str] = Field(
        default=None, max_length=640, description="Display name of the user."
    )
    created_at: str = Field(
        ..., description="The timestamp of when the record was created on Bluesky."
    )
    text: str = Field(..., description="The text of the record.")
    embed: Optional[Union[ProcessedEmbed, str]] = Field(
        default=None, description="The embeds in the record, if any."
    )
    entities: Optional[str] = Field(
        default=None,
        description="The entities of the record, if any. Separated by a separator.",
    )
    facets: Optional[str] = Field(
        default=None,
        description="The facets of the record, if any. Separated by a separator.",
    )
    labels: Optional[str] = Field(
        default=None,
        description="The labels of the record, if any. Separated by a separator.",
    )
    langs: Optional[str] = Field(
        default=None, description="The languages of the record, if specified."
    )
    reply_parent: Optional[str] = Field(
        default=None,
        description="The parent post that the record is responding to in the thread, if any.",
    )
    reply_root: Optional[str] = Field(
        default=None, description="The root post of the thread, if any."
    )
    tags: Optional[str] = Field(
        default=None, description="The tags of the record, if any."
    )
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")
    url: Optional[str] = Field(
        default=None,
        description="The URL of the post. Available only if the post is from feed view. Firehose posts won't have this hydrated.",
    )
    source: te.Literal["firehose", "most_liked"] = Field(
        ...,
        description="The source feed of the post. Either 'firehose' or 'most_liked'",
    )
    like_count: Optional[int] = Field(
        default=None, description="The like count of the post."
    )
    reply_count: Optional[int] = Field(
        default=None, description="The reply count of the post."
    )
    repost_count: Optional[int] = Field(
        default=None, description="The repost count of the post."
    )
    passed_filters: bool = Field(
        ..., description="Indicates if the post passed the filters"
    )
    filtered_at: str = Field(..., description="Timestamp when the post was filtered")
    filtered_by_func: Optional[str] = Field(
        default=None, description="Function used to filter the post"
    )
    preprocessing_timestamp: str = Field(
        ..., description="Timestamp when the post was preprocessed"
    )

    # Fields from SociopoliticalLabelsModel
    llm_model_name: Optional[str] = Field(
        default=None, description="Name of LLM model used for inference."
    )
    sociopolitical_was_successfully_labeled: bool = Field(
        ...,
        description="Indicates if the post was successfully labeled by the LLM.",
    )
    sociopolitical_reason: Optional[str] = Field(
        default=None,
        description="Reason for why the post was not labeled successfully by the LLM.",
    )
    sociopolitical_label_timestamp: str = Field(
        ...,
        description="Timestamp when the post was labeled by the LLM (or, if labeling failed, when it was attempted).",
    )
    is_sociopolitical: Optional[bool] = Field(
        default=None, description="The sociopolitical label of the post."
    )
    political_ideology_label: Optional[str] = Field(
        default=None,
        description="If the post is sociopolitical, the political ideology label of the post.",
    )

    # Fields from PerspectiveApiLabelsModel
    perspective_was_successfully_labeled: bool = Field(
        ...,
        description="Indicates if the post was successfully labeled by the Perspective API.",
    )
    perspective_reason: Optional[str] = Field(
        default=None,
        description="Reason for why the post was not labeled successfully by the Perspective API.",
    )
    perspective_label_timestamp: str = Field(
        ...,
        description="Timestamp when the post was labeled by the Perspective API (or, if labeling failed, when it was attempted).",
    )
    prob_toxic: Optional[float] = Field(
        default=None, description="Probability of toxicity."
    )
    prob_severe_toxic: Optional[float] = Field(
        default=None, description="Probability of severe toxicity."
    )
    prob_identity_attack: Optional[float] = Field(
        default=None, description="Probability of identity attack."
    )
    prob_insult: Optional[float] = Field(
        default=None, description="Probability of insult."
    )
    prob_profanity: Optional[float] = Field(
        default=None, description="Probability of profanity."
    )
    prob_threat: Optional[float] = Field(
        default=None, description="Probability of threat."
    )
    prob_affinity: Optional[float] = Field(
        default=None, description="Probability of affinity."
    )
    prob_compassion: Optional[float] = Field(
        default=None, description="Probability of compassion."
    )
    prob_constructive: Optional[float] = Field(
        default=None, description="Probability of constructive."
    )
    prob_curiosity: Optional[float] = Field(
        default=None, description="Probability of curiosity."
    )
    prob_nuance: Optional[float] = Field(
        default=None, description="Probability of nuance."
    )
    prob_personal_story: Optional[float] = Field(
        default=None, description="Probability of personal story."
    )
    prob_reasoning: Optional[float] = Field(
        default=None, description="Probability of reasoning."
    )
    prob_respect: Optional[float] = Field(
        default=None, description="Probability of respect."
    )
    prob_alienation: Optional[float] = Field(
        default=None, description="Probability of alienation."
    )
    prob_fearmongering: Optional[float] = Field(
        default=None, description="Probability of fearmongering."
    )
    prob_generalization: Optional[float] = Field(
        default=None, description="Probability of generalization."
    )
    prob_moral_outrage: Optional[float] = Field(
        default=None, description="Probability of moral outrage."
    )
    prob_scapegoating: Optional[float] = Field(
        default=None, description="Probability of scapegoating."
    )
    prob_sexually_explicit: Optional[float] = Field(
        default=None, description="Probability of sexually explicit."
    )
    prob_flirtation: Optional[float] = Field(
        default=None, description="Probability of flirtation."
    )
    prob_spam: Optional[float] = Field(default=None, description="Probability of spam.")

    # Fields from similarity_scores_results. This will only exist
    # for the in-network posts. The most-liked posts will not have
    # similarity scores.
    similarity_score: Optional[float] = Field(
        default=None, description="Cosine similarity score of the post."
    )
    most_liked_average_embedding_key: Optional[str] = Field(
        default=None,
        description="S3 key of the average most liked feed embedding used for comparison.",
    )
    # consolidation-specific logic
    consolidation_timestamp: str = Field(
        ...,
        description="Timestamp when the post was consolidated.",
    )
