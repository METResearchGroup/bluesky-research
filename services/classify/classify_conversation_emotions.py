"""Classify emotions within conversations.

We use the same attributes that are used in Google's Perspective API
(https://perspectiveapi.com/how-it-works/), which are:

- TOXICITY: A rude, disrespectful, or unreasonable comment that is likely to make people leave a discussion.
- AFFINITY_EXPERIMENTAL: References shared interests, motivations, or outlooks between the comment author and another individual, group, or identity.
- COMPASSION_EXPERIMENTAL: Identifies with or shows concern, empathy, or support for the feelings/emotions of others.
- CONSTRUCTIVE_EXPERIMENTAL: Makes specific or well-reasoned points to provide a fuller understanding of the topic without disrespect or provocation.
- CURIOSITY_EXPERIMENTAL: Attempts to clarify or ask follow-up questions to better understand another person or idea.
- NUANCE_EXPERIMENTAL: Incorporates multiple points of view in an attempt to provide a full picture or contribute useful detail and/or content.
- PERSONAL_STORY_EXPERIMENTAL: Includes a personal experience or story as a source of support for the statements made in the comment.
- ALIENATION_EXPERIMENTAL: Portrays someone as inferior, implies a lack of belonging, or frames the statement in an us vs. them context.
- FEARMONGERING_EXPERIMENTAL: Deliberately arouses fear or alarm about a particular issue.
- GENERALIZATION_EXPERIMENTAL: Asserts something to be true for either of all members of a certain group or of an indefinite part of that group.
- MORAL_OUTRAGE_EXPERIMENTAL: Anger, disgust, or frustration directed toward other people or entities who seem to violate the author's ethical values or standards.
- POWER_APPEAL_EXPERIMENTAL: Makes note of one individual, group, or entity having power over the behavior and outcomes of another.
- SCAPEGOATING_EXPERIMENTAL: Blames a person or entity for the wrongdoings, mistakes, or faults of others, especially for reasons of expediency.
"""
from typing import Literal

from services.classify.models.llm import perform_inference as llm_inference
from services.classify.models.perspective_api import perform_inference as perspective_inference # noqa

model_to_inference_func_map = {
    "perspective_api": perspective_inference,
    "llm": llm_inference
}

def classify_text_batch(
    batch: list[str],
    model: Literal["perspective_api", "llm"] = "llm"
) -> list[dict]:
    """Classify emotions within a conversation.

    Takes as input a list of texts in a batch form and returns a list of JSONs.
    """
    inference_func = model_to_inference_func_map[model]
    return inference_func(batch=batch)


def classify_text_batches(
    text_batches: list[list[str]],
    model: Literal["perspective_api", "llm"] = "llm"
) -> list[dict]:
    """Classify emotions within conversations.

    Takes as input a list of text batches, and classifies each batch
    using the specified model and attributes.
    """
    text_labels: list[dict] = []
    for batch in text_batches:
        text_labels.extend(classify_text_batch(batch=batch, model=model))
    return text_labels
