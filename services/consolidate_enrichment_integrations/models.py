"""Pydantic models for the consolidated enrichment integrations."""

from pydantic import BaseModel, Field

import typing_extensions as te

from lib.db.bluesky_models.embed import ProcessedEmbed


class ConsolidatedEnrichedPostModel(BaseModel):
    """Pydantic model for the consolidated enriched post."""

    # Fields from FilteredPreprocessedPostModel
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: str | None = Field(
        default=None,
        description="The timestamp of when the post was indexed by Bluesky.",
    )
    author_did: str = Field(..., description="The DID of the user.")
    author_handle: str | None = Field(
        default=None, description="The handle of the user."
    )
    author_avatar: str | None = None
    author_display_name: str | None = Field(
        default=None, max_length=640, description="Display name of the user."
    )
    created_at: str = Field(
        ..., description="The timestamp of when the record was created on Bluesky."
    )
    text: str = Field(..., description="The text of the record.")
    embed: ProcessedEmbed | str | None = Field(
        default=None, description="The embeds in the record, if any."
    )
    entities: str | None = Field(
        default=None,
        description="The entities of the record, if any. Separated by a separator.",
    )
    facets: str | None = Field(
        default=None,
        description="The facets of the record, if any. Separated by a separator.",
    )
    labels: str | None = Field(
        default=None,
        description="The labels of the record, if any. Separated by a separator.",
    )
    langs: str | None = Field(
        default=None, description="The languages of the record, if specified."
    )
    reply_parent: str | None = Field(
        default=None,
        description="The parent post that the record is responding to in the thread, if any.",
    )
    reply_root: str | None = Field(
        default=None, description="The root post of the thread, if any."
    )
    tags: str | None = Field(
        default=None, description="The tags of the record, if any."
    )
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")
    url: str | None = Field(
        default=None,
        description="The URL of the post. Available only if the post is from feed view. Firehose posts won't have this hydrated.",
    )
    source: te.Literal["firehose", "most_liked"] = Field(
        ...,
        description="The source feed of the post. Either 'firehose' or 'most_liked'",
    )
    like_count: int | None = Field(
        default=None, description="The like count of the post."
    )
    reply_count: int | None = Field(
        default=None, description="The reply count of the post."
    )
    repost_count: int | None = Field(
        default=None, description="The repost count of the post."
    )
    passed_filters: bool = Field(
        ..., description="Indicates if the post passed the filters"
    )
    filtered_at: str = Field(..., description="Timestamp when the post was filtered")
    filtered_by_func: str | None = Field(
        default=None, description="Function used to filter the post"
    )
    preprocessing_timestamp: str = Field(
        ..., description="Timestamp when the post was preprocessed"
    )

    # Fields from SociopoliticalLabelsModel
    llm_model_name: str | None = Field(
        default=None, description="Name of LLM model used for inference."
    )
    sociopolitical_was_successfully_labeled: bool = Field(
        ...,
        description="Indicates if the post was successfully labeled by the LLM.",
    )
    sociopolitical_reason: str | None = Field(
        default=None,
        description="Reason for why the post was not labeled successfully by the LLM.",
    )
    sociopolitical_label_timestamp: str | None = Field(
        ...,
        description="Timestamp when the post was labeled by the LLM (or, if labeling failed, when it was attempted).",
    )
    is_sociopolitical: bool | None = Field(
        default=None, description="The sociopolitical label of the post."
    )
    political_ideology_label: str | None = Field(
        default=None,
        description="If the post is sociopolitical, the political ideology label of the post.",
    )

    # Fields from PerspectiveApiLabelsModel
    perspective_was_successfully_labeled: bool = Field(
        ...,
        description="Indicates if the post was successfully labeled by the Perspective API.",
    )
    perspective_reason: str | None = Field(
        default=None,
        description="Reason for why the post was not labeled successfully by the Perspective API.",
    )
    perspective_label_timestamp: str | None = Field(
        ...,
        description="Timestamp when the post was labeled by the Perspective API (or, if labeling failed, when it was attempted).",
    )
    prob_toxic: float | None = Field(
        default=None, description="Probability of toxicity."
    )
    prob_severe_toxic: float | None = Field(
        default=None, description="Probability of severe toxicity."
    )
    prob_identity_attack: float | None = Field(
        default=None, description="Probability of identity attack."
    )
    prob_insult: float | None = Field(
        default=None, description="Probability of insult."
    )
    prob_profanity: float | None = Field(
        default=None, description="Probability of profanity."
    )
    prob_threat: float | None = Field(
        default=None, description="Probability of threat."
    )
    prob_affinity: float | None = Field(
        default=None, description="Probability of affinity."
    )
    prob_compassion: float | None = Field(
        default=None, description="Probability of compassion."
    )
    prob_constructive: float | None = Field(
        default=None, description="Probability of constructive."
    )
    prob_curiosity: float | None = Field(
        default=None, description="Probability of curiosity."
    )
    prob_nuance: float | None = Field(
        default=None, description="Probability of nuance."
    )
    prob_personal_story: float | None = Field(
        default=None, description="Probability of personal story."
    )
    prob_reasoning: float | None = Field(
        default=None, description="Probability of reasoning."
    )
    prob_respect: float | None = Field(
        default=None, description="Probability of respect."
    )
    prob_alienation: float | None = Field(
        default=None, description="Probability of alienation."
    )
    prob_fearmongering: float | None = Field(
        default=None, description="Probability of fearmongering."
    )
    prob_generalization: float | None = Field(
        default=None, description="Probability of generalization."
    )
    prob_moral_outrage: float | None = Field(
        default=None, description="Probability of moral outrage."
    )
    prob_scapegoating: float | None = Field(
        default=None, description="Probability of scapegoating."
    )
    prob_sexually_explicit: float | None = Field(
        default=None, description="Probability of sexually explicit."
    )
    prob_flirtation: float | None = Field(
        default=None, description="Probability of flirtation."
    )
    prob_spam: float | None = Field(default=None, description="Probability of spam.")

    # Fields from similarity_scores_results. This will only exist
    # for the in-network posts. The most-liked posts will not have
    # similarity scores.
    similarity_score: float | None = Field(
        default=None, description="Cosine similarity score of the post."
    )
    most_liked_average_embedding_key: str | None = Field(
        default=None,
        description="S3 key of the average most liked feed embedding used for comparison.",
    )
    # consolidation-specific logic
    consolidation_timestamp: str = Field(
        ...,
        description="Timestamp when the post was consolidated.",
    )
