# VADER sentiment thresholds for valence classification

VADER_POSITIVE_THRESHOLD: float = 0.05
VADER_NEGATIVE_THRESHOLD: float = -0.05
VADER_NEUTRAL_RANGE: tuple[float, float] = (
    VADER_NEGATIVE_THRESHOLD,
    VADER_POSITIVE_THRESHOLD,
)

# Label names for output
VALENCE_LABELS = ["positive", "neutral", "negative"]
