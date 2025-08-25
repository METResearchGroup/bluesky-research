"""Example usage of shared processing modules.

This script demonstrates how to use the extracted processing functions
to replace the monolithic logic from the original scripts.
"""

import pandas as pd

# Import shared processing modules
from services.calculate_analytics.study_analytics.shared.processing.features import (
    calculate_feature_averages,
    calculate_feature_proportions,
)
from services.calculate_analytics.study_analytics.shared.processing.thresholds import (
    map_date_to_static_week,
    map_date_to_dynamic_week,
)
from services.calculate_analytics.study_analytics.shared.processing.utils import (
    calculate_probability_threshold_proportions,
    safe_mean,
    validate_probability_series,
)


def example_feature_calculation():
    """Example of using feature calculation functions."""
    print("=== Feature Calculation Example ===")

    # Create sample post data
    sample_posts = pd.DataFrame(
        {
            "prob_toxic": [0.1, 0.8, 0.3, 0.9, 0.2],
            "prob_severe_toxic": [0.05, 0.7, 0.1, 0.8, 0.1],
            "prob_identity_attack": [0.1, 0.6, 0.2, 0.7, 0.1],
            "prob_insult": [0.2, 0.8, 0.3, 0.9, 0.2],
            "prob_profanity": [0.1, 0.7, 0.2, 0.8, 0.1],
            "prob_threat": [0.05, 0.6, 0.1, 0.7, 0.05],
            "prob_affinity": [0.8, 0.2, 0.7, 0.1, 0.9],
            "prob_compassion": [0.9, 0.1, 0.8, 0.2, 0.9],
            "prob_constructive": [0.7, 0.3, 0.6, 0.2, 0.8],
            "prob_curiosity": [0.8, 0.2, 0.7, 0.3, 0.9],
            "prob_nuance": [0.6, 0.4, 0.5, 0.3, 0.7],
            "prob_personal_story": [0.7, 0.3, 0.6, 0.2, 0.8],
            "prob_reasoning": [0.8, 0.2, 0.7, 0.3, 0.9],
            "prob_respect": [0.9, 0.1, 0.8, 0.2, 0.9],
            "prob_alienation": [0.2, 0.8, 0.3, 0.9, 0.2],
            "prob_fearmongering": [0.1, 0.7, 0.2, 0.8, 0.1],
            "prob_generalization": [0.2, 0.8, 0.3, 0.9, 0.2],
            "prob_moral_outrage": [0.1, 0.7, 0.2, 0.8, 0.1],
            "prob_scapegoating": [0.05, 0.6, 0.1, 0.7, 0.05],
            "prob_sexually_explicit": [0.1, 0.7, 0.2, 0.8, 0.1],
            "prob_flirtation": [0.2, 0.8, 0.3, 0.9, 0.2],
            "prob_spam": [0.1, 0.6, 0.2, 0.7, 0.1],
            "prob_emotion": [0.8, 0.2, 0.7, 0.3, 0.9],
            "prob_intergroup": [0.7, 0.3, 0.6, 0.2, 0.8],
            "prob_moral": [0.8, 0.2, 0.7, 0.3, 0.9],
            "prob_other": [0.1, 0.7, 0.2, 0.8, 0.1],
            "is_sociopolitical": [0.8, 0.9, 0.7, 0.9, 0.6],
            "political_ideology_label": [
                "left",
                "right",
                "moderate",
                "left",
                "unclear",
            ],
            "valence_label": [
                "positive",
                "negative",
                "neutral",
                "negative",
                "positive",
            ],
        }
    )

    # Calculate feature averages
    averages = calculate_feature_averages(sample_posts, "user123")
    print("Feature averages for user123:")
    print(f"  Average toxicity: {averages['avg_prob_toxic']:.3f}")
    print(f"  Average compassion: {averages['avg_prob_compassion']:.3f}")
    print(f"  Political content: {averages['avg_is_political']:.3f}")
    print(f"  Positive valence: {averages['avg_is_positive']:.3f}")

    # Calculate feature proportions with threshold
    proportions = calculate_feature_proportions(sample_posts, "user123", threshold=0.6)
    print("\nFeature proportions (threshold=0.6) for user123:")
    print(f"  Toxic posts: {proportions['prop_toxic_posts']:.3f}")
    print(f"  Compassionate posts: {proportions['prop_compassion_posts']:.3f}")
    print(f"  Political posts: {proportions['prop_is_political']:.3f}")

    return averages, proportions


def example_threshold_calculation():
    """Example of using threshold calculation functions."""
    print("\n=== Threshold Calculation Example ===")

    # Example date mapping
    week = map_date_to_static_week("2024-10-15", 1)
    print(f"Date 2024-10-15 maps to week {week} for wave 1")

    # Example dynamic week mapping
    dynamic_thresholds = ["2024-10-10", "2024-10-20", "2024-10-30"]
    dynamic_week = map_date_to_dynamic_week("2024-10-25", dynamic_thresholds)
    print(f"Date 2024-10-25 maps to dynamic week {dynamic_week}")

    # Note: These would require actual configuration and data to run
    print("Static week thresholds calculation would require study configuration")
    print("Dynamic week thresholds calculation would require Qualtrics data")

    return week, dynamic_week


def example_engagement_analysis():
    """Example of using engagement analysis functions."""
    print("\n=== Engagement Analysis Example ===")

    # Example users
    users = [{"bluesky_handle": "user1"}, {"bluesky_handle": "user2"}]

    # Example partition dates
    partition_dates = ["2024-10-01", "2024-10-02", "2024-10-03"]

    # Note: These would require actual data loading to run
    print("Engagement analysis would require actual data from local storage")
    print("Functions available:")
    print("  - get_num_records_per_user_per_day(record_type)")
    print("  - aggregate_metrics_per_user_per_day(users, partition_dates)")
    print("  - get_engagement_summary_per_user(users, partition_dates)")

    return users, partition_dates


def example_utility_functions():
    """Example of using utility functions."""
    print("\n=== Utility Functions Example ===")

    # Create sample probability series
    prob_series = pd.Series([0.1, 0.3, 0.7, 0.9, 0.2, 0.8])

    # Validate probability series
    is_valid = validate_probability_series(prob_series)
    print(f"Probability series is valid: {is_valid}")

    # Calculate threshold proportions
    proportion_above_05 = calculate_probability_threshold_proportions(prob_series, 0.5)
    proportion_above_07 = calculate_probability_threshold_proportions(prob_series, 0.7)
    print(f"Proportion above 0.5: {proportion_above_05:.3f}")
    print(f"Proportion above 0.7: {proportion_above_07:.3f}")

    # Safe mean calculation
    mean_val = safe_mean(prob_series)
    print(f"Mean value: {mean_val:.3f}")

    # Handle empty series
    empty_series = pd.Series([])
    safe_mean_empty = safe_mean(empty_series, default=-1.0)
    print(f"Mean of empty series (with default): {safe_mean_empty}")

    return prob_series, proportion_above_05, mean_val


def example_integration():
    """Example of integrating multiple processing modules."""
    print("\n=== Integration Example ===")

    # This would be a complete analysis pipeline
    print("Complete analysis pipeline would include:")
    print("1. Load data using shared.data_loading modules")
    print("2. Process features using shared.processing.features")
    print("3. Calculate thresholds using shared.processing.thresholds")
    print("4. Analyze engagement using shared.processing.engagement")
    print("5. Combine results using shared.processing.utils")

    print("\nExample integration code:")
    print("""
from shared.data_loading import load_posts_with_labels, load_study_users
from shared.processing.features import calculate_feature_averages
from shared.processing.thresholds import get_week_thresholds_per_user_static
from shared.processing.engagement import get_engagement_summary_per_user

# Load data
posts = load_posts_with_labels(partition_date="2024-10-15")
users = load_study_users()

# Process features
user_features = {}
for user in users:
    user_posts = posts[posts['author'] == user['bluesky_handle']]
    user_features[user['bluesky_handle']] = calculate_feature_averages(user_posts, user['bluesky_handle'])

# Get week thresholds
week_thresholds = get_week_thresholds_per_user_static(users)

# Analyze engagement
engagement_summary = get_engagement_summary_per_user(users, ["2024-10-15"])

# Combine results
results = {
    'features': user_features,
    'week_assignments': week_thresholds,
    'engagement': engagement_summary
}
    """)


def main():
    """Run all examples."""
    print("Shared Processing Modules - Example Usage")
    print("=" * 50)

    try:
        # Run examples
        example_feature_calculation()
        example_threshold_calculation()
        example_engagement_analysis()
        example_utility_functions()
        example_integration()

        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print("\nKey benefits of using shared processing modules:")
        print("✅ Eliminates code duplication across scripts")
        print("✅ Provides consistent processing patterns")
        print("✅ Integrates with shared configuration system")
        print("✅ Includes comprehensive error handling")
        print("✅ Maintains backward compatibility")
        print("✅ Enables gradual migration from monolithic scripts")

    except Exception as e:
        print(f"\nError running examples: {e}")
        print("This may be due to missing configuration or data dependencies")


if __name__ == "__main__":
    main()
