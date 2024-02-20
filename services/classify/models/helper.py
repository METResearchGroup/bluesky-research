"""Helper functions for classification models."""
DEFAULT_INFERENCE_FIELDS = [
    "TOXICITY", "AFFINITY_EXPERIMENTAL", "COMPASSION_EXPERIMENTAL",
    "CONSTRUCTIVE_EXPERIMENTAL", "CURIOSITY_EXPERIMENTAL", "NUANCE_EXPERIMENTAL", # noqa
    "PERSONAL_STORY_EXPERIMENTAL", "ALIENATION_EXPERIMENTAL", "FEARMONGERING_EXPERIMENTAL", # noqa
    "GENERALIZATION_EXPERIMENTAL", "MORAL_OUTRAGE_EXPERIMENTAL", "POWER_APPEAL_EXPERIMENTAL", # noqa
    "SCAPEGOATING_EXPERIMENTAL"
]

template = """
%INSTRUCTIONS:
Please classify the following text (in the "%TEXT" block) using the following attributes:
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


Please return in a list of JSONs, where each JSON is in the following format:
{{
    "text": <text>,
    "TOXICITY": <binary 0 or 1>,
    "AFFINITY_EXPERIMENTAL": <binary 0 or 1>,
    "COMPASSION_EXPERIMENTAL": <binary 0 or 1>,
    "CONSTRUCTIVE_EXPERIMENTAL": <binary 0 or 1>,
    "CURIOSITY_EXPERIMENTAL": <binary 0 or 1>,
    "NUANCE_EXPERIMENTAL": <binary 0 or 1>,
    "PERSONAL_STORY_EXPERIMENTAL": <binary 0 or 1>,
    "ALIENATION_EXPERIMENTAL": <binary 0 or 1>,
    "FEARMONGERING_EXPERIMENTAL": <binary 0 or 1>,
    "GENERALIZATION_EXPERIMENTAL": <binary 0 or 1>,
    "MORAL_OUTRAGE_EXPERIMENTAL": <binary 0 or 1>,
    "POWER_APPEAL_EXPERIMENTAL": <binary 0 or 1>,
    "SCAPEGOATING_EXPERIMENTAL": <binary 0 or 1>
}}

Example input:
Texts:
1. "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts"]

Example desired output:
[
    {{
        "text": "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
        "TOXICITY": 0,
        "AFFINITY_EXPERIMENTAL": 0,
        "COMPASSION_EXPERIMENTAL": 1,
        "CONSTRUCTIVE_EXPERIMENTAL": 0,
        "CURIOSITY_EXPERIMENTAL": 0,
        "NUANCE_EXPERIMENTAL": 0,
        "PERSONAL_STORY_EXPERIMENTAL": 1,
        "ALIENATION_EXPERIMENTAL": 0,
        "FEARMONGERING_EXPERIMENTAL": 0,
        "GENERALIZATION_EXPERIMENTAL": 0,
        "MORAL_OUTRAGE_EXPERIMENTAL": 0,
        "POWER_APPEAL_EXPERIMENTAL": 0,
        "SCAPEGOATING_EXPERIMENTAL": 0
    }}
]

For a list of texts passed in, please return a corresponding list of JSONs.
Example input:
Texts:
1. "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
2. "An annoying thing about being a parent in Germany is receiving unsolicited parenting criticism from random strangers on the street (your baby's hat is not warm enough! He's leaning his head too far backwards!, etc). It's always women who do this. Would it be fair to call it complaining?"


Example output:
[
    {{
        "text": "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
        "TOXICITY": 0,
        "AFFINITY_EXPERIMENTAL": 0,
        "COMPASSION_EXPERIMENTAL": 1,
        "CONSTRUCTIVE_EXPERIMENTAL": 0,
        "CURIOSITY_EXPERIMENTAL": 0,
        "NUANCE_EXPERIMENTAL": 0,
        "PERSONAL_STORY_EXPERIMENTAL": 1,
        "ALIENATION_EXPERIMENTAL": 0,
        "FEARMONGERING_EXPERIMENTAL": 0,
        "GENERALIZATION_EXPERIMENTAL": 0,
        "MORAL_OUTRAGE_EXPERIMENTAL": 0,
        "POWER_APPEAL_EXPERIMENTAL": 0,
        "SCAPEGOATING_EXPERIMENTAL": 0
    }},
    {{
        "text": "An annoying thing about being a parent in Germany is receiving unsolicited parenting criticism from random strangers on the street (your baby's hat is not warm enough!, He's leaning his head too far backwards!, etc). It's always women who do this. Would it be fair to call it complaining?",
        "TOXICITY": 0,
        "AFFINITY_EXPERIMENTAL": 0,
        "COMPASSION_EXPERIMENTAL": 1,
        "CONSTRUCTIVE_EXPERIMENTAL": 0,
        "CURIOSITY_EXPERIMENTAL": 1,
        "NUANCE_EXPERIMENTAL": 0,
        "PERSONAL_STORY_EXPERIMENTAL": 1,
        "ALIENATION_EXPERIMENTAL": 1,
        "FEARMONGERING_EXPERIMENTAL": 0,
        "GENERALIZATION_EXPERIMENTAL": 1,
        "MORAL_OUTRAGE_EXPERIMENTAL": 0,
        "POWER_APPEAL_EXPERIMENTAL": 0,
        "SCAPEGOATING_EXPERIMENTAL": 0
    }}
]

After generating each JSON, validate that each is a properly formatted JSON. If any JSON\
is not properly formatted, please properly format it.

%TEXT:
{text}
"""
