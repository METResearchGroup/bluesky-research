import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict


class TopNEntities:
    """Container for top N entities with frequency counts."""

    def __init__(self, entities_dict: Dict[str, int] = None):
        self.entities_dict = entities_dict or {}

    def get_top_n(self, n: int = 10) -> Dict[str, int]:
        """Get top N entities by frequency."""
        return dict(Counter(self.entities_dict).most_common(n))

    def get_all_entities(self) -> Dict[str, int]:
        """Get all entities with their counts."""
        return self.entities_dict.copy()

    def add_entity(self, entity: str, count: int = 1):
        """Add entity with count."""
        self.entities_dict[entity] = self.entities_dict.get(entity, 0) + count


def normalize_entity(entity_text: str) -> str:
    """Normalize entity text for consistent analysis."""
    import re

    # Case-folding and basic normalization
    normalized = entity_text.lower().strip()
    # Remove surrounding punctuation
    normalized = re.sub(r"^[^\w\s]+|[^\w\s]+$", "", normalized)
    return normalized


def aggregate_entities_by_condition_and_pre_post(
    uri_to_entities_map: dict[str, list[dict[str, str]]],
    user_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
    top_n: int = 20,
    election_cutoff_date: str = "2024-11-05",
) -> Dict:
    """
    Aggregate entities by condition and pre/post-election periods.

    Returns structured data suitable for visualization similar to experiments.py
    """

    # Initialize containers
    condition_entities = defaultdict(lambda: defaultdict(int))
    election_period_entities = defaultdict(lambda: defaultdict(int))
    condition_election_entities = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    overall_entities = defaultdict(int)

    # Get user conditions mapping
    user_conditions = {}
    for _, row in user_df.iterrows():
        user_conditions[row["user_did"]] = row["condition"]

    # Process each user's feeds
    for user_did, feeds_by_date in user_to_content_in_feeds.items():
        condition = user_conditions.get(user_did)
        if not condition:
            continue

        for feed_date, post_uris in feeds_by_date.items():
            # Determine election period
            feed_datetime = datetime.strptime(feed_date, "%Y-%m-%d")
            cutoff_datetime = datetime.strptime(election_cutoff_date, "%Y-%m-%d")
            election_period = (
                "pre_election" if feed_datetime <= cutoff_datetime else "post_election"
            )

            # Process entities for posts in this feed
            for uri in post_uris:
                entities = uri_to_entities_map.get(uri, [])
                for entity_dict in entities:
                    entity_normalized = normalize_entity(
                        entity_dict.get("entity_normalized", "")
                    )
                    if not entity_normalized:
                        continue

                    # Aggregate by condition
                    condition_entities[condition][entity_normalized] += 1

                    # Aggregate by election period
                    election_period_entities[election_period][entity_normalized] += 1

                    # Aggregate by condition x election period
                    condition_election_entities[condition][election_period][
                        entity_normalized
                    ] += 1

                    # Overall aggregation
                    overall_entities[entity_normalized] += 1

    # Create TopNEntities objects with top N entities
    def create_top_n_objects(entities_dict: Dict[str, int]) -> TopNEntities:
        top_entities = dict(Counter(entities_dict).most_common(top_n))
        return TopNEntities(top_entities)

    # Build structured result similar to experiments.py output
    result = {
        "condition": {
            "reverse_chronological": create_top_n_objects(
                condition_entities["reverse_chronological"]
            ),
            "engagement": create_top_n_objects(condition_entities["engagement"]),
            "representative_diversification": create_top_n_objects(
                condition_entities["representative_diversification"]
            ),
        },
        "election_date": {
            "pre_election": create_top_n_objects(
                election_period_entities["pre_election"]
            ),
            "post_election": create_top_n_objects(
                election_period_entities["post_election"]
            ),
            "condition": {
                "reverse_chronological": {
                    "pre_election": create_top_n_objects(
                        condition_election_entities["reverse_chronological"][
                            "pre_election"
                        ]
                    ),
                    "post_election": create_top_n_objects(
                        condition_election_entities["reverse_chronological"][
                            "post_election"
                        ]
                    ),
                },
                "engagement": {
                    "pre_election": create_top_n_objects(
                        condition_election_entities["engagement"]["pre_election"]
                    ),
                    "post_election": create_top_n_objects(
                        condition_election_entities["engagement"]["post_election"]
                    ),
                },
                "representative_diversification": {
                    "pre_election": create_top_n_objects(
                        condition_election_entities["representative_diversification"][
                            "pre_election"
                        ]
                    ),
                    "post_election": create_top_n_objects(
                        condition_election_entities["representative_diversification"][
                            "post_election"
                        ]
                    ),
                },
            },
        },
        "overall": create_top_n_objects(overall_entities),
        # Add raw data for ranking comparisons
        "raw_data": {
            "condition_entities": dict(condition_entities),
            "election_period_entities": dict(election_period_entities),
            "condition_election_entities": dict(condition_election_entities),
            "overall_entities": dict(overall_entities),
        },
    }

    return result


def create_ranking_comparison(election_date_data: Dict) -> Dict:
    """
    Create ranking comparison between pre/post election periods.
    Similar to compare_pre_post_rankings in experiments.py
    """
    pre_entities = list(election_date_data["pre_election"].get_top_n(10).keys())
    post_entities = list(election_date_data["post_election"].get_top_n(10).keys())

    comparison = {"pre_to_post": {}, "post_to_pre": {}}

    # For each pre-election top 10 entity, find its rank in post-election
    for i, entity in enumerate(pre_entities):
        rank_in_pre = i + 1
        rank_in_post = (
            post_entities.index(entity) + 1
            if entity in post_entities
            else "Not in top 10"
        )
        comparison["pre_to_post"][entity] = {
            "pre_rank": rank_in_pre,
            "post_rank": rank_in_post,
            "change": rank_in_pre - rank_in_post
            if isinstance(rank_in_post, int)
            else "N/A",
        }

    # For each post-election top 10 entity, find its rank in pre-election
    for i, entity in enumerate(post_entities):
        rank_in_post = i + 1
        rank_in_pre = (
            pre_entities.index(entity) + 1
            if entity in pre_entities
            else "Not in top 10"
        )
        comparison["post_to_pre"][entity] = {
            "post_rank": rank_in_post,
            "pre_rank": rank_in_pre,
            "change": rank_in_post - rank_in_pre
            if isinstance(rank_in_pre, int)
            else "N/A",
        }

    return comparison
