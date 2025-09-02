"""Mock data for testing main.py functionality.

This module contains comprehensive test data that matches the expected schema
for user feed analysis testing.
"""

import pandas as pd

# Mock user data
mock_user_df = pd.DataFrame({
    "did": ["user1", "user2", "user3"],
    "handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
    "condition": ["control", "treatment", "control"],
    "study_start_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
    "study_end_date": ["2024-01-31", "2024-01-31", "2024-01-31"],
})

# Mock user date to week mapping
mock_user_date_to_week_df = pd.DataFrame({
    "did": ["user1", "user1", "user1", "user2", "user2", "user2", "user3", "user3", "user3"],
    "partition_date": ["2024-09-30", "2024-10-01", "2024-10-02", "2024-09-30", "2024-10-01", "2024-10-02", "2024-09-30", "2024-10-01", "2024-10-02"],
    "week": ["1", "1", "1", "1", "1", "1", "1", "1", "1"],
})

# Mock valid study users
mock_valid_study_users_dids = ["user1", "user2", "user3"]

# Mock user to content in feeds mapping
mock_user_to_content_in_feeds = {
    "user1": {
        "2024-09-30": {"post1", "post2", "post3"},
        "2024-10-01": {"post4", "post5"},
        "2024-10-02": {"post6", "post7", "post8"},
    },
    "user2": {
        "2024-09-30": {"post9", "post10"},
        "2024-10-01": {"post11", "post12", "post13"},
        "2024-10-02": {"post14"},
    },
    "user3": {
        "2024-09-30": {"post15", "post16"},
        "2024-10-01": {"post17"},
        "2024-10-02": {"post18", "post19", "post20"},
    },
}

# Mock labels for feed content (comprehensive coverage of all label types)
mock_labels_for_feed_content = {
    "post1": {
        # Perspective API labels (probability type)
        "prob_toxic": 0.8,
        "prob_constructive": 0.7,
        "prob_severe_toxic": 0.9,
        "prob_identity_attack": 0.2,
        "prob_insult": 0.6,
        "prob_profanity": 0.1,
        "prob_threat": 0.3,
        "prob_affinity": 0.8,
        "prob_compassion": 0.9,
        "prob_curiosity": 0.7,
        "prob_nuance": 0.6,
        "prob_personal_story": 0.8,
        "prob_reasoning": 0.7,
        "prob_respect": 0.9,
        "prob_alienation": 0.2,
        "prob_fearmongering": 0.1,
        "prob_generalization": 0.4,
        "prob_moral_outrage": 0.3,
        "prob_scapegoating": 0.1,
        "prob_sexually_explicit": 0.0,
        "prob_flirtation": 0.2,
        "prob_spam": 0.1,
        # IME labels (probability type)
        "prob_intergroup": 0.7,
        "prob_moral": 0.8,
        "prob_emotion": 0.6,
        "prob_other": 0.3,
        # Valence classifier labels
        "valence_clf_score": 0.7,
        "is_valence_positive": True,
        "is_valence_negative": False,
        "is_valence_neutral": False,
        # LLM classifier labels
        "is_sociopolitical": True,
        "is_not_sociopolitical": False,
        "is_political_left": True,
        "is_political_right": False,
        "is_political_moderate": False,
        "is_political_unclear": False,
    },
    "post2": {
        # Perspective API labels (probability type)
        "prob_toxic": 0.3,
        "prob_constructive": 0.9,
        "prob_severe_toxic": 0.1,
        "prob_identity_attack": 0.1,
        "prob_insult": 0.2,
        "prob_profanity": 0.0,
        "prob_threat": 0.1,
        "prob_affinity": 0.6,
        "prob_compassion": 0.8,
        "prob_curiosity": 0.9,
        "prob_nuance": 0.8,
        "prob_personal_story": 0.7,
        "prob_reasoning": 0.8,
        "prob_respect": 0.9,
        "prob_alienation": 0.1,
        "prob_fearmongering": 0.0,
        "prob_generalization": 0.2,
        "prob_moral_outrage": 0.1,
        "prob_scapegoating": 0.0,
        "prob_sexually_explicit": 0.0,
        "prob_flirtation": 0.1,
        "prob_spam": 0.0,
        # IME labels (probability type)
        "prob_intergroup": 0.5,
        "prob_moral": 0.7,
        "prob_emotion": 0.8,
        "prob_other": 0.2,
        # Valence classifier labels
        "valence_clf_score": -0.8,
        "is_valence_positive": False,
        "is_valence_negative": True,
        "is_valence_neutral": False,
        # LLM classifier labels
        "is_sociopolitical": False,
        "is_not_sociopolitical": True,
        "is_political_left": False,
        "is_political_right": False,
        "is_political_moderate": True,
        "is_political_unclear": False,
    },
    "post3": {
        # Perspective API labels (probability type)
        "prob_toxic": 0.7,
        "prob_constructive": 0.6,
        "prob_severe_toxic": 0.4,
        "prob_identity_attack": 0.3,
        "prob_insult": 0.5,
        "prob_profanity": 0.2,
        "prob_threat": 0.2,
        "prob_affinity": 0.7,
        "prob_compassion": 0.6,
        "prob_curiosity": 0.8,
        "prob_nuance": 0.7,
        "prob_personal_story": 0.6,
        "prob_reasoning": 0.6,
        "prob_respect": 0.8,
        "prob_alienation": 0.3,
        "prob_fearmongering": 0.2,
        "prob_generalization": 0.5,
        "prob_moral_outrage": 0.4,
        "prob_scapegoating": 0.2,
        "prob_sexually_explicit": 0.1,
        "prob_flirtation": 0.3,
        "prob_spam": 0.1,
        # IME labels (probability type)
        "prob_intergroup": 0.6,
        "prob_moral": 0.6,
        "prob_emotion": 0.7,
        "prob_other": 0.4,
        # Valence classifier labels
        "valence_clf_score": 0.2,
        "is_valence_positive": False,
        "is_valence_negative": False,
        "is_valence_neutral": True,
        # LLM classifier labels
        "is_sociopolitical": True,
        "is_not_sociopolitical": False,
        "is_political_left": False,
        "is_political_right": True,
        "is_political_moderate": False,
        "is_political_unclear": False,
    },
    # Additional posts with varied data for comprehensive testing
    "post4": {
        "prob_toxic": 0.9,
        "prob_constructive": 0.4,
        "prob_severe_toxic": 0.8,
        "prob_identity_attack": 0.6,
        "prob_insult": 0.8,
        "prob_profanity": 0.5,
        "prob_threat": 0.7,
        "prob_affinity": 0.3,
        "prob_compassion": 0.4,
        "prob_curiosity": 0.5,
        "prob_nuance": 0.4,
        "prob_personal_story": 0.3,
        "prob_reasoning": 0.4,
        "prob_respect": 0.5,
        "prob_alienation": 0.7,
        "prob_fearmongering": 0.6,
        "prob_generalization": 0.8,
        "prob_moral_outrage": 0.7,
        "prob_scapegoating": 0.6,
        "prob_sexually_explicit": 0.4,
        "prob_flirtation": 0.5,
        "prob_spam": 0.3,
        "prob_intergroup": 0.8,
        "prob_moral": 0.4,
        "prob_emotion": 0.3,
        "prob_other": 0.7,
        "valence_clf_score": -0.5,
        "is_valence_positive": False,
        "is_valence_negative": True,
        "is_valence_neutral": False,
        "is_sociopolitical": True,
        "is_not_sociopolitical": False,
        "is_political_left": True,
        "is_political_right": False,
        "is_political_moderate": False,
        "is_political_unclear": True,
    },
    "post5": {
        "prob_toxic": 0.2,
        "prob_constructive": 0.8,
        "prob_severe_toxic": 0.1,
        "prob_identity_attack": 0.1,
        "prob_insult": 0.2,
        "prob_profanity": 0.0,
        "prob_threat": 0.1,
        "prob_affinity": 0.8,
        "prob_compassion": 0.9,
        "prob_curiosity": 0.8,
        "prob_nuance": 0.7,
        "prob_personal_story": 0.8,
        "prob_reasoning": 0.7,
        "prob_respect": 0.9,
        "prob_alienation": 0.1,
        "prob_fearmongering": 0.0,
        "prob_generalization": 0.2,
        "prob_moral_outrage": 0.1,
        "prob_scapegoating": 0.0,
        "prob_sexually_explicit": 0.0,
        "prob_flirtation": 0.1,
        "prob_spam": 0.0,
        "prob_intergroup": 0.4,
        "prob_moral": 0.6,
        "prob_emotion": 0.7,
        "prob_other": 0.3,
        "valence_clf_score": 0.6,
        "is_valence_positive": True,
        "is_valence_negative": False,
        "is_valence_neutral": False,
        "is_sociopolitical": False,
        "is_not_sociopolitical": True,
        "is_political_left": False,
        "is_political_right": False,
        "is_political_moderate": True,
        "is_political_unclear": False,
    },
}

# Mock partition dates (using constants from shared.constants)
mock_partition_dates = ["2024-09-30", "2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-05", "2024-10-06"]

# Mock daily content per user metrics (expected output from get_daily_feed_content_per_user_metrics)
mock_user_per_day_content_label_metrics = {
    "user1": {
        "2024-09-30": {
            "feed_average_toxic": 0.6,
            "feed_proportion_toxic": 0.667,
            "feed_average_constructive": 0.733,
            "feed_proportion_constructive": 1.0,
            "feed_average_is_sociopolitical": 0.667,
            "feed_proportion_is_sociopolitical": 0.667,
            "feed_average_valence_clf_score": 0.033,
            "feed_proportion_valence_clf_score": None,
        },
        "2024-10-01": {
            "feed_average_toxic": 0.55,
            "feed_proportion_toxic": 0.5,
            "feed_average_constructive": 0.6,
            "feed_proportion_constructive": 0.5,
            "feed_average_is_sociopolitical": 0.5,
            "feed_proportion_is_sociopolitical": 0.5,
            "feed_average_valence_clf_score": 0.05,
            "feed_proportion_valence_clf_score": None,
        },
        "2024-10-02": {
            "feed_average_toxic": 0.7,
            "feed_proportion_toxic": 1.0,
            "feed_average_constructive": 0.5,
            "feed_proportion_constructive": 0.333,
            "feed_average_is_sociopolitical": 1.0,
            "feed_proportion_is_sociopolitical": 1.0,
            "feed_average_valence_clf_score": 0.2,
            "feed_proportion_valence_clf_score": None,
        },
    },
    "user2": {
        "2024-09-30": {
            "feed_average_toxic": 0.5,
            "feed_proportion_toxic": 0.5,
            "feed_average_constructive": 0.65,
            "feed_proportion_constructive": 0.5,
            "feed_average_is_sociopolitical": 0.5,
            "feed_proportion_is_sociopolitical": 0.5,
            "feed_average_valence_clf_score": -0.1,
            "feed_proportion_valence_clf_score": None,
        },
        "2024-10-01": {
            "feed_average_toxic": 0.4,
            "feed_proportion_toxic": 0.333,
            "feed_average_constructive": 0.7,
            "feed_proportion_constructive": 0.667,
            "feed_average_is_sociopolitical": 0.333,
            "feed_proportion_is_sociopolitical": 0.333,
            "feed_average_valence_clf_score": 0.1,
            "feed_proportion_valence_clf_score": None,
        },
        "2024-10-02": {
            "feed_average_toxic": 0.6,
            "feed_proportion_toxic": 1.0,
            "feed_average_constructive": 0.4,
            "feed_proportion_constructive": 0.0,
            "feed_average_is_sociopolitical": 1.0,
            "feed_proportion_is_sociopolitical": 1.0,
            "feed_average_valence_clf_score": -0.3,
            "feed_proportion_valence_clf_score": None,
        },
    },
    "user3": {
        "2024-09-30": {
            "feed_average_toxic": 0.45,
            "feed_proportion_toxic": 0.5,
            "feed_average_constructive": 0.75,
            "feed_proportion_constructive": 1.0,
            "feed_average_is_sociopolitical": 0.5,
            "feed_proportion_is_sociopolitical": 0.5,
            "feed_average_valence_clf_score": 0.15,
            "feed_proportion_valence_clf_score": None,
        },
        "2024-10-01": {
            "feed_average_toxic": 0.3,
            "feed_proportion_toxic": 0.0,
            "feed_average_constructive": 0.8,
            "feed_proportion_constructive": 1.0,
            "feed_average_is_sociopolitical": 0.0,
            "feed_proportion_is_sociopolitical": 0.0,
            "feed_average_valence_clf_score": 0.6,
            "feed_proportion_valence_clf_score": None,
        },
        "2024-10-02": {
            "feed_average_toxic": 0.55,
            "feed_proportion_toxic": 0.667,
            "feed_average_constructive": 0.6,
            "feed_proportion_constructive": 0.667,
            "feed_average_is_sociopolitical": 0.667,
            "feed_proportion_is_sociopolitical": 0.667,
            "feed_average_valence_clf_score": 0.1,
            "feed_proportion_valence_clf_score": None,
        },
    },
}

# Mock weekly content per user metrics (expected output from get_weekly_content_per_user_metrics)
mock_user_per_week_content_label_metrics = {
    "user1": {
        "1": {
            "feed_average_toxic": 0.617,
            "feed_proportion_toxic": 0.722,
            "feed_average_constructive": 0.611,
            "feed_proportion_constructive": 0.611,
            "feed_average_is_sociopolitical": 0.722,
            "feed_proportion_is_sociopolitical": 0.722,
            "feed_average_valence_clf_score": 0.094,
            "feed_proportion_valence_clf_score": None,
        },
    },
    "user2": {
        "1": {
            "feed_average_toxic": 0.5,
            "feed_proportion_toxic": 0.611,
            "feed_average_constructive": 0.583,
            "feed_proportion_constructive": 0.389,
            "feed_average_is_sociopolitical": 0.611,
            "feed_proportion_is_sociopolitical": 0.611,
            "feed_average_valence_clf_score": -0.1,
            "feed_proportion_valence_clf_score": None,
        },
    },
    "user3": {
        "1": {
            "feed_average_toxic": 0.433,
            "feed_proportion_toxic": 0.389,
            "feed_average_constructive": 0.717,
            "feed_proportion_constructive": 0.889,
            "feed_average_is_sociopolitical": 0.389,
            "feed_proportion_is_sociopolitical": 0.389,
            "feed_average_valence_clf_score": 0.283,
            "feed_proportion_valence_clf_score": None,
        },
    },
}

# Mock transformed daily metrics DataFrame
mock_transformed_per_user_per_day_content_label_metrics = pd.DataFrame({
    "did": ["user1", "user1", "user1", "user2", "user2", "user2", "user3", "user3", "user3"],
    "partition_date": ["2024-09-30", "2024-10-01", "2024-10-02", "2024-09-30", "2024-10-01", "2024-10-02", "2024-09-30", "2024-10-01", "2024-10-02"],
    "handle": ["user1.bsky.social", "user1.bsky.social", "user1.bsky.social", "user2.bsky.social", "user2.bsky.social", "user2.bsky.social", "user3.bsky.social", "user3.bsky.social", "user3.bsky.social"],
    "condition": ["control", "control", "control", "treatment", "treatment", "treatment", "control", "control", "control"],
    "feed_average_toxic": [0.6, 0.55, 0.7, 0.5, 0.4, 0.6, 0.45, 0.3, 0.55],
    "feed_proportion_toxic": [0.667, 0.5, 1.0, 0.5, 0.333, 1.0, 0.5, 0.0, 0.667],
    "feed_average_constructive": [0.733, 0.6, 0.5, 0.65, 0.7, 0.4, 0.75, 0.8, 0.6],
    "feed_proportion_constructive": [1.0, 0.5, 0.333, 0.5, 0.667, 0.0, 1.0, 1.0, 0.667],
    "feed_average_is_sociopolitical": [0.667, 0.5, 1.0, 0.5, 0.333, 1.0, 0.5, 0.0, 0.667],
    "feed_proportion_is_sociopolitical": [0.667, 0.5, 1.0, 0.5, 0.333, 1.0, 0.5, 0.0, 0.667],
    "feed_average_valence_clf_score": [0.033, 0.05, 0.2, -0.1, 0.1, -0.3, 0.15, 0.6, 0.1],
    "feed_proportion_valence_clf_score": [None, None, None, None, None, None, None, None, None],
})

# Mock transformed weekly metrics DataFrame
mock_transformed_per_user_per_week_feed_content_metrics = pd.DataFrame({
    "did": ["user1", "user2", "user3"],
    "week": ["1", "1", "1"],
    "handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
    "condition": ["control", "treatment", "control"],
    "feed_average_toxic": [0.617, 0.5, 0.433],
    "feed_proportion_toxic": [0.722, 0.611, 0.389],
    "feed_average_constructive": [0.611, 0.583, 0.717],
    "feed_proportion_constructive": [0.611, 0.389, 0.889],
    "feed_average_is_sociopolitical": [0.722, 0.611, 0.389],
    "feed_proportion_is_sociopolitical": [0.722, 0.611, 0.389],
    "feed_average_valence_clf_score": [0.094, -0.1, 0.283],
    "feed_proportion_valence_clf_score": [None, None, None],
})

# Mock setup objects dictionary
mock_setup_objs = {
    "user_df": mock_user_df,
    "user_date_to_week_df": mock_user_date_to_week_df,
    "valid_study_users_dids": mock_valid_study_users_dids,
    "user_to_content_in_feeds": mock_user_to_content_in_feeds,
    "labels_for_feed_content": mock_labels_for_feed_content,
    "partition_dates": mock_partition_dates,
}
