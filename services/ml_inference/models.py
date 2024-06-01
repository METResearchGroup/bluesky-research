"""Pydantic models for storing Perspective API labels."""
from pydantic import BaseModel, Field, validator
from typing import Optional
import typing_extensions as te


class RecordClassificationMetadataModel(BaseModel):
    """Metadata for the classification of a record.

    Returns key identifiers for a post (e.g., URI) as well as some other
    metadata and information useful for filtering and for analysis.

    This is a duplicate of data already available in other tables, but stored
    here for convenient joins with labels for analysis.
    """
    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    preprocessing_timestamp: str = Field(..., description="The timestamp when the post was processed.")  # noqa
    source: te.Literal["firehose", "most_liked"] = Field(..., description="The source feed of the post. Either 'firehose' or 'most_liked'")  # noqa
    url: Optional[str] = Field(
        default=None,
        description="The URL of the post. Available only if the post is from feed view. Firehose posts won't have this hydrated." # noqa
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


class PerspectiveApiLabelsModel(BaseModel):
    """Stores results of classifications from Perspective API.

    Uses all the available attributes from the Perspective API, so that we have
    this data in the future as well for exploratory analysis.
    """
    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the post.")
    was_successfully_labeled: bool = Field(..., description="Indicates if the post was successfully labeled by the Perspective API.")
    reason: Optional[str] = Field(default=None, description="Reason for why the post was not labeled successfully.")
    label_timestamp: str = Field(..., description="Timestamp when the post was labeled (or, if labeling failed, when it was attempted).")
    prob_toxic: Optional[float] = Field(default=None, description="Probability of toxicity.")
    prob_severe_toxic: Optional[float] = Field(default=None, description="Probability of severe toxicity.")
    prob_identity_attack: Optional[float] = Field(default=None, description="Probability of identity attack.")
    prob_insult: Optional[float] = Field(default=None, description="Probability of insult.")
    prob_profanity: Optional[float] = Field(default=None, description="Probability of profanity.")
    prob_threat: Optional[float] = Field(default=None, description="Probability of threat.")
    prob_affinity: Optional[float] = Field(default=None, description="Probability of affinity.")
    prob_compassion: Optional[float] = Field(default=None, description="Probability of compassion.")
    prob_constructive: Optional[float] = Field(default=None, description="Probability of constructive.")
    prob_curiosity: Optional[float] = Field(default=None, description="Probability of curiosity.")
    prob_nuance: Optional[float] = Field(default=None, description="Probability of nuance.")
    prob_personal_story: Optional[float] = Field(default=None, description="Probability of personal story.")
    prob_reasoning: Optional[float] = Field(default=None, description="Probability of reasoning.")
    prob_respect: Optional[float] = Field(default=None, description="Probability of respect.")
    prob_alienation: Optional[float] = Field(default=None, description="Probability of alienation.")
    prob_fearmongering: Optional[float] = Field(default=None, description="Probability of fearmongering.")
    prob_generalization: Optional[float] = Field(default=None, description="Probability of generalization.")
    prob_moral_outrage: Optional[float] = Field(default=None, description="Probability of moral outrage.")
    prob_scapegoating: Optional[float] = Field(default=None, description="Probability of scapegoating.")
    prob_sexually_explicit: Optional[float] = Field(default=None, description="Probability of sexually explicit.")
    prob_flirtation: Optional[float] = Field(default=None, description="Probability of flirtation.")
    prob_spam: Optional[float] = Field(default=None, description="Probability of spam.")


class LLMSociopoliticalLabelModel(BaseModel):
    """The LLM output for a single post."""
    is_sociopolitical: bool = Field(..., description="The sociopolitical label of the post.") # noqa
    political_ideology_label: Optional[str] = Field(default=None, description="If the post is sociopolitical, the political ideology label of the post.") # noqa
    reason_sociopolitical: str = Field(..., description="Reason from the LLM for its sociopolitical label.") # noqa
    reason_political_ideology: Optional[str] = Field(default=None, description="Reason from the LLM for its political ideology label.") # noqa

    @validator("political_ideology_label", "reason_political_ideology")
    def validate_political_ideology_label(cls, v, values):
        """Checks to see if we do have a sociopolitical post, that we then
        have a political ideology label and reason. If we don't have a
        sociopolitical post, then we shouldn't have a political ideology.
        """
        if values["is_sociopolitical"] and not v:
            raise ValueError("If the post is sociopolitical, the political ideology label and reason must be provided.") # noqa
        if not values["is_sociopolitical"] and v:
            raise ValueError("If the post is not sociopolitical, the political ideology label and reason must not be provided.") # noqa
        return v


class LLMSociopoliticalOutputModel(BaseModel):
    """Verifies the output of the LLM for sociopolitical classification."""
    results: list[LLMSociopoliticalLabelModel] = Field(..., description="The results of the LLM for sociopolitical classification.") # noqa
    count: int = Field(..., description="The number of posts classified. Must match the expected number of posts.") # noqa

    @validator("results")
    def validate_results(cls, v, values):
        """Ensure that the number of results matches the count field in the
        model."""
        if len(v) != values["count"]:
            llm_actual_results = len(v)
            llm_stated_results = values["count"]
            raise ValueError(
                f"The number of results ({llm_actual_results}) must match the count field ({llm_stated_results})." # noqa
            )


class SociopoliticalLabelsModel(BaseModel):
    """Stores results of sociopolitical and political ideology labels from
    the LLM."""
    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the post.")
    llm_model_name: Optional[str] = Field(default=None, description="Name of LLM model used for inference.") # noqa
    was_successfully_labeled: bool = Field(..., description="Indicates if the post was successfully labeled by the Perspective API.") # noqa
    reason: Optional[str] = Field(default=None, description="Reason for why the post was not labeled successfully.") # noqa
    label_timestamp: str = Field(..., description="Timestamp when the post was labeled (or, if labeling failed, when it was attempted).") # noqa
    is_sociopolitical: Optional[bool] = Field(default=None, description="The sociopolitical label of the post.") # noqa
    political_ideology_label: Optional[str] = Field(default=None, description="If the post is sociopolitical, the political ideology label of the post.") # noqa
    reason_sociopolitical: Optional[str] = Field(default=None, description="Reason from the LLM for its sociopolitical label.") # noqa
    reason_political_ideology: Optional[str] = Field(default=None, description="Reason from the LLM for its political ideology label.") # noqa

    @validator("political_ideology_label", "reason_political_ideology")
    def validate_political_ideology_label(cls, v, values):
        """Checks to see if we do have a sociopolitical post, that we then
        have a political ideology label and reason. If we don't have a
        sociopolitical post, then we shouldn't have a political ideology.
        """
        if values["is_sociopolitical"] and not v:
            raise ValueError("If the post is sociopolitical, the political ideology label and reason must be provided.") # noqa
        if not values["is_sociopolitical"] and v:
            raise ValueError("If the post is not sociopolitical, the political ideology label and reason must not be provided.") # noqa
        return v
