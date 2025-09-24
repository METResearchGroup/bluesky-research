"""Transformations for NER."""

import re

import spacy

# Entity normalization mappings (common aliases)
ENTITY_ALIASES = {
    # People
    "joseph biden": "joe biden",
    "kamala harris": "kamala harris",
    "elizabeth warren": "elizabeth warren",
    "ron desantis": "ron desantis",
    "eric adams": "eric adams",
    "anthony blinken": "antony blinken",
    "ted cruz": "ted cruz",
    "gavin newsom": "gavin newsom",
    "lori lightfoot": "lori lightfoot",
    "donald trump": "donald trump",
    # Organizations
    "supreme court": "supreme court",
    "federal reserve": "federal reserve",
    "pentagon": "pentagon",
    "white house": "white house",
    "department of justice": "department of justice",
    "doj": "department of justice",
    # Locations
    "united states": "usa",
    "usa": "usa",
    "us": "usa",
    "new york city": "new york",
    "nyc": "new york",
    "california": "california",
    "ca": "california",
    "texas": "texas",
    "tx": "texas",
    "florida": "florida",
    "fl": "florida",
    "massachusetts": "massachusetts",
    "ma": "massachusetts",
    "mississippi": "mississippi",
    "ms": "mississippi",
    "arizona": "arizona",
    "az": "arizona",
    "georgia": "georgia",
    "ga": "georgia",
    "chicago": "chicago",
    "brussels": "brussels",
    "afghanistan": "afghanistan",
    "european union": "european union",
    "eu": "european union",
}


def normalize_entity(entity_text: str) -> str:
    """
    Normalize entity text using case-folding, punctuation stripping, and alias mapping.

    Args:
        entity_text: Raw entity text from spaCy

    Returns:
        Normalized entity text
    """
    # Case-folding
    normalized = entity_text.lower()

    # Strip surrounding punctuation
    normalized = re.sub(r"^[^\w\s]+|[^\w\s]+$", "", normalized)

    # Map common aliases
    normalized = ENTITY_ALIASES.get(normalized, normalized)

    return normalized.strip()


def get_entities_for_post(post: str) -> list[dict[str, str]]:
    """Retrieves the entities for a given post.

    Returns the entity as well as entity type.
    """
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        print(
            "Model en_core_web_sm not found. Please install with: python -m spacy download en_core_web_sm"
        )
        return []

    # Focus on political/sociopolitical entity types
    target_types = {"PERSON", "ORG", "GPE", "DATE"}

    doc = nlp(post)
    entities = []

    for ent in doc.ents:
        if ent.label_ in target_types:
            entity_info = {
                "entity_type": ent.label_,
                "entity_normalized": normalize_entity(ent.text),
            }
            entities.append(entity_info)

    return entities


def get_entities_for_posts(
    uri_to_text: dict[str, str],
) -> dict[str, list[dict[str, str]]]:
    """Extract entities for all posts using spaCy NER."""
    uri_to_entities_map: dict[str, list[dict[str, str]]] = {}

    print(f"Processing {len(uri_to_text)} posts for entity extraction...")

    for i, (uri, text) in enumerate(uri_to_text.items()):
        if i % 1000 == 0:  # Progress indicator
            print(f"Processed {i}/{len(uri_to_text)} posts...")

        entities: list[dict[str, str]] = get_entities_for_post(text)
        uri_to_entities_map[uri] = entities

    print(f"Entity extraction complete. Processed {len(uri_to_entities_map)} posts.")
    return uri_to_entities_map
