"""Classifies if a post is civic or not."""
THRESHOLD = 0.5

def text_only_classification(text: str) -> tuple:
    return (None, None)


def rag_only_classification(text: str) -> tuple:
    return (None, None)


def classify_text_as_civic(text: str) -> dict:
    """Classifies a text as civic or not.

    Args:
        text: The text to classify.

    Returns:
        A dictionary with the classification.
        :text: The text to classify.
        :prob: The probability that the text is civic.
        :label: The label of the classification.
    """

    probs, label = text_only_classification(text)
    # NOTE: need something besides "if the text isn't civic, double-check
    # via RAG, since this is too simplistic."
    if label == 0:
        rag_probs, rag_label = rag_only_classification(text)

    return {
        "text": text,
        "civic": 1
    }
