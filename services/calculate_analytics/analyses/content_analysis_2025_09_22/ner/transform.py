import pandas as pd


class TopNEntities:
    pass


def aggregate_entities_by_condition_and_pre_post(
    uri_to_entities_map: dict[str, list[dict[str, str]]],
    user_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
    top_n: int = 20,
):
    """Slice entities by condition and pre/post-election."""
    return {
        "condition": {
            "reverse_chronological": TopNEntities(),
            "engagement": TopNEntities(),
            "representative_diversification": TopNEntities(),
        },
        "election_date": {
            "pre_election": TopNEntities(),
            "post_election": TopNEntities(),
            "condition": {
                "reverse_chronological": TopNEntities(),
                "engagement": TopNEntities(),
                "representative_diversification": TopNEntities(),
            },
        },
        "overall": TopNEntities(),
    }
