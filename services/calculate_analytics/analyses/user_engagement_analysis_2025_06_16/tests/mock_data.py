"""Mock data for testing user engagement analysis main.py functionality.

This module contains comprehensive test data that matches the expected schema
for user engagement analysis testing.
"""

import pandas as pd

from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    TOTAL_STUDY_WEEKS_PER_USER,
)

# Mock user data
mock_user_df = pd.DataFrame({
    "did": ["user1", "user2", "user3"],
    "handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
    "study_group": ["treatment", "control", "treatment"],
    "enrollment_date": ["2024-09-30", "2024-10-01", "2024-10-02"],
})

# Mock user date to week mapping - using week numbers as strings "1", "2", etc.
mock_user_date_to_week_df = pd.DataFrame({
    "did": ["user1", "user1", "user2", "user2", "user3"],
    "date": ["2024-09-30", "2024-10-01", "2024-09-30", "2024-10-01", "2024-09-30"],
    "week": ["1", "1", "1", "1", "1"],  # Week 1 for all dates in this example
})

# Mock valid study users DIDs
mock_valid_study_users_dids = ["user1", "user2", "user3"]

# Mock partition dates - using actual study dates from constants
mock_partition_dates = ["2024-09-30", "2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04"]

# Mock engaged content data (keyed on post URI)
mock_engaged_content = {
    "post1": [
        {
            "uri": "post1",
            "did": "user1",
            "date": "2024-09-30",
            "record_type": "like",
            "engagement_timestamp": "2024-09-30T10:00:00Z"
        },
        {
            "uri": "post1",
            "did": "user2",
            "date": "2024-09-30",
            "record_type": "repost",
            "engagement_timestamp": "2024-09-30T11:00:00Z"
        }
    ],
    "post2": [
        {
            "uri": "post2",
            "did": "user1",
            "date": "2024-10-01",
            "record_type": "like",
            "engagement_timestamp": "2024-10-01T10:00:00Z"
        }
    ],
    "post3": [
        {
            "uri": "post3",
            "did": "user3",
            "date": "2024-09-30",
            "record_type": "reply",
            "engagement_timestamp": "2024-09-30T12:00:00Z"
        }
    ]
}

# Mock user to content engaged with data (keyed on user DID, then date)
mock_user_to_content_engaged_with = {
    "user1": {
        "2024-09-30": {
            "like": ["post1"],
            "repost": [],
            "reply": []
        },
        "2024-10-01": {
            "like": ["post2"],
            "repost": [],
            "reply": []
        }
    },
    "user2": {
        "2024-09-30": {
            "like": [],
            "repost": ["post1"],
            "reply": []
        }
    },
    "user3": {
        "2024-09-30": {
            "like": [],
            "repost": [],
            "reply": ["post3"]
        }
    }
}

# Mock labels for engaged content (keyed on post URI)
mock_labels_for_engaged_content = {
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
    }
}

# Mock daily content per user metrics
mock_user_per_day_content_label_metrics = {
    "user1": {
        "2024-09-30": {
            "prob_toxic": 0.8,
            "prob_constructive": 0.7,
            "is_sociopolitical": True,
            "valence_clf_score": 0.7,
        },
        "2024-10-01": {
            "prob_toxic": 0.3,
            "prob_constructive": 0.9,
            "is_sociopolitical": False,
            "valence_clf_score": -0.8,
        }
    },
    "user2": {
        "2024-09-30": {
            "prob_toxic": 0.8,
            "prob_constructive": 0.7,
            "is_sociopolitical": True,
            "valence_clf_score": 0.7,
        }
    },
    "user3": {
        "2024-09-30": {
            "prob_toxic": 0.7,
            "prob_constructive": 0.6,
            "is_sociopolitical": True,
            "valence_clf_score": 0.2,
        }
    }
}

# Mock weekly content per user metrics - using week numbers as strings
mock_user_per_week_content_label_metrics = {
    "user1": {
        "1": {  # Week 1
            "prob_toxic": 0.55,
            "prob_constructive": 0.8,
            "is_sociopolitical": 0.5,
            "valence_clf_score": -0.05,
        }
    },
    "user2": {
        "1": {  # Week 1
            "prob_toxic": 0.8,
            "prob_constructive": 0.7,
            "is_sociopolitical": 1.0,
            "valence_clf_score": 0.7,
        }
    },
    "user3": {
        "1": {  # Week 1
            "prob_toxic": 0.7,
            "prob_constructive": 0.6,
            "is_sociopolitical": 1.0,
            "valence_clf_score": 0.2,
        }
    }
}

# Mock transformed daily metrics DataFrame
mock_transformed_per_user_per_day_content_label_metrics = pd.DataFrame({
    "did": ["user1", "user1", "user2", "user3"],
    "date": ["2024-09-30", "2024-10-01", "2024-09-30", "2024-09-30"],
    "prob_toxic": [0.8, 0.3, 0.8, 0.7],
    "prob_constructive": [0.7, 0.9, 0.7, 0.6],
    "is_sociopolitical": [True, False, True, True],
    "valence_clf_score": [0.7, -0.8, 0.7, 0.2],
})

# Mock transformed weekly metrics DataFrame - using week numbers as strings
mock_transformed_user_per_week_content_label_metrics = pd.DataFrame({
    "did": ["user1", "user2", "user3"],
    "week": ["1", "1", "1"],  # Week 1 for all users
    "prob_toxic": [0.55, 0.8, 0.7],
    "prob_constructive": [0.8, 0.7, 0.6],
    "is_sociopolitical": [0.5, 1.0, 1.0],
    "valence_clf_score": [-0.05, 0.7, 0.2],
})

# Mock setup objects for testing
mock_setup_objs = {
    "user_df": mock_user_df,
    "user_date_to_week_df": mock_user_date_to_week_df,
    "user_to_content_engaged_with": mock_user_to_content_engaged_with,
    "labels_for_engaged_content": mock_labels_for_engaged_content,
    "partition_dates": mock_partition_dates,
}
