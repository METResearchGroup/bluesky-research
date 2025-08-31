"""Mock data for testing labels.py functionality.

This module contains comprehensive test data that matches the expected schema
from models.py for each integration type.
"""

import pandas as pd

# Mock data for perspective_api integration (matches PerspectiveApiLabelsModel)
mock_perspective_data = pd.DataFrame({
    "uri": ["post1", "post2"],
    "text": ["Sample text 1", "Sample text 2"],
    "preprocessing_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "was_successfully_labeled": [True, True],
    "reason": [None, None],
    "label_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "prob_toxic": [0.8, 0.3],
    "prob_severe_toxic": [0.1, 0.0],
    "prob_identity_attack": [0.2, 0.1],
    "prob_insult": [0.3, 0.2],
    "prob_profanity": [0.1, 0.0],
    "prob_threat": [0.0, 0.0],
    "prob_affinity": [0.7, 0.8],
    "prob_compassion": [0.6, 0.7],
    "prob_constructive": [0.6, 0.9],
    "prob_curiosity": [0.8, 0.9],
    "prob_nuance": [0.5, 0.6],
    "prob_personal_story": [0.4, 0.5],
    "prob_reasoning": [0.6, 0.7],
    "prob_respect": [0.8, 0.9],
    "prob_alienation": [0.2, 0.1],
    "prob_fearmongering": [0.1, 0.0],
    "prob_generalization": [0.3, 0.2],
    "prob_moral_outrage": [0.2, 0.1],
    "prob_scapegoating": [0.1, 0.0],
    "prob_sexually_explicit": [0.0, 0.0],
    "prob_flirtation": [0.1, 0.0],
    "prob_spam": [0.0, 0.0],
})

# Mock data for sociopolitical integration (matches SociopoliticalLabelsModel)
mock_sociopolitical_data = pd.DataFrame({
    "uri": ["post1", "post2"],
    "text": ["Sample text 1", "Sample text 2"],
    "preprocessing_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "llm_model_name": ["gpt-4", "gpt-4"],
    "was_successfully_labeled": [True, True],
    "reason": [None, None],
    "label_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "is_sociopolitical": [True, False],
    "political_ideology_label": ["left", "right"],
})

# Mock data for ime integration (matches ImeLabelModel)
mock_ime_data = pd.DataFrame({
    "uri": ["post1", "post2"],
    "text": ["Sample text 1", "Sample text 2"],
    "preprocessing_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "was_successfully_labeled": [True, True],
    "prob_emotion": [0.6, 0.4],
    "prob_intergroup": [0.7, 0.5],
    "prob_moral": [0.8, 0.6],
    "prob_other": [0.3, 0.7],
    "label_emotion": [1, 0],
    "label_intergroup": [1, 0],
    "label_moral": [1, 0],
    "label_other": [0, 1],
    "label_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
})

# Mock data for valence_classifier integration (matches ValenceClassifierLabelModel)
mock_valence_data = pd.DataFrame({
    "uri": ["post1", "post2"],
    "text": ["Sample text 1", "Sample text 2"],
    "preprocessing_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "was_successfully_labeled": [True, True],
    "label_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
    "valence_label": ["positive", "negative"],
    "compound": [0.7, -0.8],
})

# Mock data for testing missing labels scenarios
mock_perspective_data_minimal = pd.DataFrame({
    "uri": ["post1", "post2"],
    "prob_toxic": [0.8, 0.3],
    "prob_constructive": [0.6, 0.9],
    "prob_severe_toxic": [0.1, 0.0],
    "prob_identity_attack": [0.2, 0.1],
    "prob_insult": [0.3, 0.2],
    "prob_profanity": [0.1, 0.0],
    "prob_threat": [0.0, 0.0],
    "prob_affinity": [0.7, 0.8],
    "prob_compassion": [0.6, 0.7],
    "prob_curiosity": [0.8, 0.9],
    "prob_nuance": [0.5, 0.6],
    "prob_personal_story": [0.4, 0.5],
    "prob_reasoning": [0.6, 0.7],
    "prob_respect": [0.8, 0.9],
    "prob_alienation": [0.2, 0.1],
    "prob_fearmongering": [0.1, 0.0],
    "prob_generalization": [0.3, 0.2],
    "prob_moral_outrage": [0.2, 0.1],
    "prob_scapegoating": [0.1, 0.0],
    "prob_sexually_explicit": [0.0, 0.0],
    "prob_flirtation": [0.1, 0.0],
    "prob_spam": [0.0, 0.0],
})

mock_sociopolitical_data_minimal = pd.DataFrame({
    "uri": ["post1", "post2"],
    "is_sociopolitical": [True, False],
    "political_ideology_label": ["left", "right"],
})

mock_ime_data_minimal = pd.DataFrame({
    "uri": ["post1", "post2"],
    "prob_intergroup": [0.7, 0.5],
    "prob_moral": [0.8, 0.6],
    "prob_emotion": [0.6, 0.4],
    "prob_other": [0.3, 0.7],
})

mock_valence_data_minimal = pd.DataFrame({
    "uri": ["post1", "post2"],
    "compound": [0.7, -0.8],
    "valence_label": ["positive", "negative"],
})

perspective_api_labels = [
    "prob_toxic", "prob_constructive", "prob_severe_toxic",
    "prob_identity_attack", "prob_insult", "prob_profanity",
    "prob_threat", "prob_affinity", "prob_compassion",
    "prob_curiosity", "prob_nuance", "prob_personal_story",
    "prob_reasoning", "prob_respect", "prob_alienation",
    "prob_fearmongering", "prob_generalization", "prob_moral_outrage",
    "prob_scapegoating", "prob_sexually_explicit", "prob_flirtation", "prob_spam"
]

sociopolitical_labels = [
    "is_sociopolitical", "is_not_sociopolitical", "is_political_left", "is_political_right", "is_political_moderate", "is_political_unclear"
]

ime_labels = [
    "prob_intergroup", "prob_moral", "prob_emotion", "prob_other"
]

valence_labels = [
    "valence_clf_score", "is_valence_positive", "is_valence_negative", "is_valence_neutral"
]
