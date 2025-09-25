"""Transformations for NER."""

import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Optional

import spacy

from lib.log.logger import get_logger

# Entity normalization mappings (common aliases)
ENTITY_ALIASES = {
    # People
    "joseph biden": "biden",
    "biden": "biden",
    "harris": "harris",
    "kamala": "harris",
    "kamala harris": "harris",
    "donald": "trump",
    "donald trump": "trump",
    "trump": "trump",
    "warren": "warren",
    "elizabeth warren": "warren",
    "ron desantis": "ron desantis",
    "eric adams": "eric adams",
    "anthony blinken": "antony blinken",
    "ted cruz": "ted cruz",
    "gavin newsom": "gavin newsom",
    "lori lightfoot": "lori lightfoot",
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
    "u.s": "usa",
    "u.s.a": "usa",
    "u.s.": "usa",
    "america": "usa",
    "the united states": "usa",
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

logger = get_logger(__file__)


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


def get_entities_for_post(post: str, nlp_model=None) -> list[dict[str, str]]:
    """Retrieves the entities for a given post.

    Args:
        post: Text content to extract entities from
        nlp_model: Pre-loaded spaCy model (optional, will load if not provided)

    Returns:
        List of entity dictionaries with 'entity_type' and 'entity_normalized' keys
    """
    if nlp_model is None:
        try:
            nlp_model = spacy.load("en_core_web_sm")
        except OSError:
            logger.info(
                "Model en_core_web_sm not found. Please install with: python -m spacy download en_core_web_sm"
            )
            return []

    # Focus on political/sociopolitical entity types
    target_types = {"PERSON", "ORG", "GPE", "DATE"}

    doc = nlp_model(post)
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
    max_workers: Optional[int] = None,
    use_parallel: bool = True,
) -> dict[str, list[dict[str, str]]]:
    """Extract entities for all posts using spaCy NER.

    Args:
        uri_to_text: Dictionary mapping post URIs to text content
        max_workers: Maximum number of worker threads (default: auto-detect)
        use_parallel: Whether to use parallel processing (default: True)

    Returns:
        Dictionary mapping post URIs to lists of entity dictionaries
    """
    if not use_parallel or len(uri_to_text) < 100:
        # Use sequential processing for small datasets
        return _get_entities_for_posts_sequential(uri_to_text)
    else:
        # Use parallel processing for larger datasets
        return _get_entities_for_posts_parallel(uri_to_text, max_workers)


def _get_entities_for_posts_sequential(
    uri_to_text: dict[str, str],
) -> dict[str, list[dict[str, str]]]:
    """Sequential entity extraction (fallback for small datasets)."""
    uri_to_entities_map: dict[str, list[dict[str, str]]] = {}

    logger.info(f"Processing {len(uri_to_text)} posts sequentially...")

    # Load model once
    try:
        nlp_model = spacy.load("en_core_web_sm")
    except OSError:
        logger.info(
            "Model en_core_web_sm not found. Please install with: python -m spacy download en_core_web_sm"
        )
        return {}

    for i, (uri, text) in enumerate(uri_to_text.items()):
        if i % 1000 == 0:  # Progress indicator
            logger.info(f"Processed {i}/{len(uri_to_text)} posts...")

        entities: list[dict[str, str]] = get_entities_for_post(text, nlp_model)
        uri_to_entities_map[uri] = entities

    logger.info(
        f"Entity extraction complete. Processed {len(uri_to_entities_map)} posts."
    )
    return uri_to_entities_map


def _get_entities_for_posts_parallel(
    uri_to_text: dict[str, str], max_workers: int = None
) -> dict[str, list[dict[str, str]]]:
    """Extract entities using parallel processing with ThreadPoolExecutor."""

    # Thread-local storage for spaCy models
    thread_local = threading.local()

    def get_nlp_model():
        """Get or create spaCy model for current thread."""
        if not hasattr(thread_local, "nlp"):
            try:
                thread_local.nlp = spacy.load("en_core_web_sm")
            except OSError:
                logger.info(
                    "Model en_core_web_sm not found. Please install with: python -m spacy download en_core_web_sm"
                )
                thread_local.nlp = None
        return thread_local.nlp

    def process_post(
        uri_text_pair: Tuple[str, str],
    ) -> Tuple[str, list[dict[str, str]]]:
        """Process a single post and return URI and entities."""
        uri, text = uri_text_pair
        try:
            nlp_model = get_nlp_model()
            if nlp_model is None:
                return uri, []

            entities = get_entities_for_post(text, nlp_model)
            return uri, entities
        except Exception as e:
            logger.info(f"Error processing post {uri}: {e}")
            return uri, []

    # Determine optimal worker count
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 1) + 4)

    logger.info(
        f"Processing {len(uri_to_text)} posts in parallel with {max_workers} workers..."
    )

    uri_to_entities_map: dict[str, list[dict[str, str]]] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_uri = {
            executor.submit(process_post, (uri, text)): uri
            for uri, text in uri_to_text.items()
        }

        # Collect results with progress tracking
        completed = 0
        for future in future_to_uri:
            if completed % 1000 == 0:  # Progress indicator
                logger.info(f"Processed {completed}/{len(uri_to_text)} posts...")

            uri, entities = future.result()
            uri_to_entities_map[uri] = entities
            completed += 1

    logger.info(
        f"Parallel entity extraction complete. Processed {len(uri_to_entities_map)} posts."
    )
    return uri_to_entities_map
