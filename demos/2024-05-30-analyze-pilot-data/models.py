"""Pydantic models for storing Perspective API labels."""
from typing import Optional

from pydantic import BaseModel, Field


class PerspectiveApiLabelModel(BaseModel):
    """Stores results of classifications from Perspective API.

    Uses all the available attributes from the Perspective API, so that we have
    this data in the future as well for exploratory analysis.
    """
    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the post.")
    was_successfully_labeled: bool = Field(
        ..., description="Indicates if the post was successfully labeled by the Perspective API.")
    reason: Optional[str] = Field(
        default=None, description="Reason for why the post was not labeled successfully.")
    label_timestamp: str = Field(
        ..., description="Timestamp when the post was labeled (or, if labeling failed, when it was attempted).")
    prob_toxic: Optional[float] = Field(
        default=None, description="Probability of toxicity.")
    label_toxic: Optional[int] = Field(
        default=None, description="Label of toxicity (0 or 1).")
    prob_severe_toxic: Optional[float] = Field(
        default=None, description="Probability of severe toxicity.")
    label_severe_toxic: Optional[int] = Field(
        default=None, description="Label of severe toxicity (0 or 1).")
    prob_identity_attack: Optional[float] = Field(
        default=None, description="Probability of identity attack.")
    label_identity_attack: Optional[int] = Field(
        default=None, description="Label of identity attack (0 or 1).")
    prob_insult: Optional[float] = Field(
        default=None, description="Probability of insult.")
    label_insult: Optional[int] = Field(
        default=None, description="Label of insult (0 or 1).")
    prob_profanity: Optional[float] = Field(
        default=None, description="Probability of profanity.")
    label_profanity: Optional[int] = Field(
        default=None, description="Label of profanity (0 or 1).")
    prob_threat: Optional[float] = Field(
        default=None, description="Probability of threat.")
    label_threat: Optional[int] = Field(
        default=None, description="Label of threat (0 or 1).")
    prob_affinity: Optional[float] = Field(
        default=None, description="Probability of affinity.")
    label_affinity: Optional[int] = Field(
        default=None, description="Label of affinity (0 or 1).")
    prob_compassion: Optional[float] = Field(
        default=None, description="Probability of compassion.")
    label_compassion: Optional[int] = Field(
        default=None, description="Label of compassion (0 or 1).")
    prob_constructive: Optional[float] = Field(
        default=None, description="Probability of constructive.")
    label_constructive: Optional[int] = Field(
        default=None, description="Label of constructive (0 or 1).")
    prob_curiosity: Optional[float] = Field(
        default=None, description="Probability of curiosity.")
    label_curiosity: Optional[int] = Field(
        default=None, description="Label of curiosity (0 or 1).")
    prob_nuance: Optional[float] = Field(
        default=None, description="Probability of nuance.")
    label_nuance: Optional[int] = Field(
        default=None, description="Label of nuance (0 or 1).")
    prob_personal_story: Optional[float] = Field(
        default=None, description="Probability of personal story.")
    label_personal_story: Optional[int] = Field(
        default=None, description="Label of personal story (0 or 1).")
    prob_reasoning: Optional[float] = Field(
        default=None, description="Probability of reasoning.")
    label_reasoning: Optional[int] = Field(
        default=None, description="Label of reasoning (0 or 1).")
    prob_respect: Optional[float] = Field(
        default=None, description="Probability of respect.")
    label_respect: Optional[int] = Field(
        default=None, description="Label of respect (0 or 1).")
    prob_alienation: Optional[float] = Field(
        default=None, description="Probability of alienation.")
    label_alienation: Optional[int] = Field(
        default=None, description="Label of alienation (0 or 1).")
    prob_fearmongering: Optional[float] = Field(
        default=None, description="Probability of fearmongering.")
    label_fearmongering: Optional[int] = Field(
        default=None, description="Label of fearmongering (0 or 1).")
    prob_generalization: Optional[float] = Field(
        default=None, description="Probability of generalization.")
    label_generalization: Optional[int] = Field(
        default=None, description="Label of generalization (0 or 1).")
    prob_moral_outrage: Optional[float] = Field(
        default=None, description="Probability of moral outrage.")
    label_moral_outrage: Optional[int] = Field(
        default=None, description="Label of moral outrage (0 or 1).")
    prob_scapegoating: Optional[float] = Field(
        default=None, description="Probability of scapegoating.")
    label_scapegoating: Optional[int] = Field(
        default=None, description="Label of scapegoating (0 or 1).")
    prob_sexually_explicit: Optional[float] = Field(
        default=None, description="Probability of sexually explicit.")
    label_sexually_explicit: Optional[int] = Field(
        default=None, description="Label of sexually explicit (0 or 1).")
    prob_flirtation: Optional[float] = Field(
        default=None, description="Probability of flirtation.")
    label_flirtation: Optional[int] = Field(
        default=None, description="Label of flirtation (0 or 1).")
    prob_spam: Optional[float] = Field(
        default=None, description="Probability of spam.")
    label_spam: Optional[int] = Field(
        default=None, description="Label of spam (0 or 1).")
