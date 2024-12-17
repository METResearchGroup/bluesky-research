from datetime import timedelta

from lib.constants import current_datetime, default_lookback_days, timestamp_format
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa
from services.rank_score_feeds.metrics import plot_lines
from services.rank_score_feeds.scoring import calculate_post_age, score_post_freshness


# copy of "load_latest_consolidated_enriched_posts" in /helper.py
def _load_latest_consolidated_enriched_posts(
    lookback_days: int = default_lookback_days,
) -> list[ConsolidatedEnrichedPostModel]:
    """Load the latest consolidated enriched posts."""
    from lib.aws.athena import Athena

    athena = Athena()
    lookback_datetime = current_datetime - timedelta(days=lookback_days)
    lookback_datetime_str = lookback_datetime.strftime(timestamp_format)
    query = f"""
    SELECT *
    FROM consolidated_enriched_post_records 
    WHERE synctimestamp > '{lookback_datetime_str}'
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    return [ConsolidatedEnrichedPostModel(**post) for post in df_dicts]


def _load_superposters() -> set[str]:
    from services.calculate_superposters.helper import load_latest_superposters

    superposter_dids: set[str] = load_latest_superposters()
    return superposter_dids


def _test():
    consolidated_enriched_posts: list[ConsolidatedEnrichedPostModel] = (
        _load_latest_consolidated_enriched_posts()
    )
    # superposter_dids: set[str] = _load_superposters()
    # post_scores: list[dict] = calculate_post_scores(
    #     posts=consolidated_enriched_posts,
    #     superposter_dids=superposter_dids
    # )
    # treatment_scores = [score["treatment_score"] for score in post_scores]
    # engagement_scores = [score["engagement_score"] for score in post_scores]
    # treatment_score_metrics = calculate_summary_statistics(treatment_scores)
    # engagement_score_metrics = calculate_summary_statistics(engagement_scores)
    # for metric in treatment_score_metrics.keys():
    #     print('#' * 8 + f" {metric.upper()} " + '#' * 8)
    #     print(f"Treatment score {metric}: {treatment_score_metrics[metric]}")
    #     print(f"Engagement score {metric}: {engagement_score_metrics[metric]}")
    post_age_hours = [calculate_post_age(post) for post in consolidated_enriched_posts]
    post_freshness_linear_scores = [
        score_post_freshness(post=post, score_func="linear")
        for post in consolidated_enriched_posts
    ]
    post_freshness_exponential_scores = [
        score_post_freshness(post=post, score_func="exponential")
        for post in consolidated_enriched_posts
    ]
    plot_lines(
        x1=post_age_hours,
        y1=post_freshness_linear_scores,
        label1="Linear freshness function",
        x2=post_age_hours,
        y2=post_freshness_exponential_scores,
        label2="Exponential freshness function",
    )
    # plot_scores(
    #     score1=post_freshness_linear_scores,
    #     label1="Linear freshness function",
    #     score2=post_freshness_exponential_scores,
    #     label2="Exponential freshness function"
    # )
    # plot_single_scores(post_freshness_scores)
    # plot_scores(engagement_scores, treatment_scores)


if __name__ == "__main__":
    _test()
