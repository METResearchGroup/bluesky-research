"""Classifies if a post is civic or not."""
THRESHOLD = 0.5

BASE_PROMPT = """
    Can you define if the content defined in <CONTENT> has "civic" content or not?

    Use the following definition of "civic":
    - 

    Here are some examples of civic content:
    {civic_examples}

    Here are some examples of content that is not civic
    {non_civic_examples}

    <CONTENT>
"""

PROMPT_WITH_CONTEXT = """

    The following in <CONTEXT> is provided as context for the content in
    <CONTENT> in order to aid with classification:

    <CONTEXT>
    {context}
"""

PROMPT_GET_MOST_RELEVANT_CONTEXT = """
    Given the content in <CONTENT> and the contexts in <CONTEXT>, please
    provide the most relevant context.

    Return the answer in JSON format.

    Example output:
    {relevant_context_output_example}
"""

def filter_relevant_context() -> str:
    """Grab only the most relevant article, if any, for a given post."""
    return ""


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
