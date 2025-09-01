LABELS_CONFIG = {
    # Perspective API labels (use threshold > 0.5)
    "prob_toxic": {
        "type": "probability",
        "threshold": 0.5,
    },
    "prob_constructive": {"type": "probability", "threshold": 0.5},
    "prob_severe_toxic": {"type": "probability", "threshold": 0.5},
    "prob_identity_attack": {"type": "probability", "threshold": 0.5},
    "prob_insult": {"type": "probability", "threshold": 0.5},
    "prob_profanity": {"type": "probability", "threshold": 0.5},
    "prob_threat": {"type": "probability", "threshold": 0.5},
    "prob_affinity": {"type": "probability", "threshold": 0.5},
    "prob_compassion": {"type": "probability", "threshold": 0.5},
    "prob_curiosity": {"type": "probability", "threshold": 0.5},
    "prob_nuance": {"type": "probability", "threshold": 0.5},
    "prob_personal_story": {"type": "probability", "threshold": 0.5},
    "prob_reasoning": {"type": "probability", "threshold": 0.5},
    "prob_respect": {"type": "probability", "threshold": 0.5},
    "prob_alienation": {"type": "probability", "threshold": 0.5},
    "prob_fearmongering": {"type": "probability", "threshold": 0.5},
    "prob_generalization": {"type": "probability", "threshold": 0.5},
    "prob_moral_outrage": {"type": "probability", "threshold": 0.5},
    "prob_scapegoating": {"type": "probability", "threshold": 0.5},
    "prob_sexually_explicit": {"type": "probability", "threshold": 0.5},
    "prob_flirtation": {"type": "probability", "threshold": 0.5},
    "prob_spam": {"type": "probability", "threshold": 0.5},
    # IME labels (use threshold > 0.5)
    "prob_intergroup": {"type": "probability", "threshold": 0.5},
    "prob_moral": {"type": "probability", "threshold": 0.5},
    "prob_emotion": {"type": "probability", "threshold": 0.5},
    "prob_other": {"type": "probability", "threshold": 0.5},
    # Valence classifier labels
    "valence_clf_score": {"type": "score"},
    "is_valence_positive": {"type": "boolean"},
    "is_valence_negative": {"type": "boolean"},
    "is_valence_neutral": {"type": "boolean"},
    # LLM classifier labels
    "is_sociopolitical": {"type": "boolean"},
    "is_not_sociopolitical": {"type": "boolean"},
    "is_political_left": {"type": "boolean"},
    "is_political_right": {"type": "boolean"},
    "is_political_moderate": {"type": "boolean"},
    "is_political_unclear": {"type": "boolean"},
}

EXPECTED_TOTAL_METRICS = (
    len(LABELS_CONFIG.keys()) * 2
)  # 2 because we calculate average and proportion for each label
