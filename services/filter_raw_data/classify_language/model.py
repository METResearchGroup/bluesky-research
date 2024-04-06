"""Model for the classify_language service.

We want to check if the language of a post is English.
"""
import fasttext

model = fasttext.load_model('lid.176.bin')


def classify(text: str) -> bool:
    """Classifies if a text is English or not."""
    return model.predict(text)[0][0] == "__label__eng_Latn"
