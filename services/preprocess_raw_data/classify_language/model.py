"""Model for the classify_language service.

We want to check if the language of a post is English.

We use our own model (`fasttext`) for this, though it looks like Bluesky (as of 2024-04-08)
is trying to classify the language of every post on their end
- https://github.com/bluesky-social/atproto/pull/2301
- https://github.com/bluesky-social/atproto/pull/2161/

We intentionally treat this model as authoritative and run it regardless of any
upstream `langs` labels, because we've observed false positives (e.g. non-English
posts labeled as "en").
"""  # noqa
import os

current_file_directory = os.path.dirname(os.path.abspath(__file__))
binary_filename = "lid.176.bin"
fp = os.path.join(current_file_directory, binary_filename)
_model = None


def _get_model():
    global _model
    if _model is not None:
        return _model
    try:
        import fasttext  # type: ignore
    except ModuleNotFoundError as e:  # pragma: no cover
        raise ModuleNotFoundError(
            "fasttext is required for language classification. "
            "Install it (e.g. from `services/preprocess_raw_data/classify_language/requirements.txt`) "
            "or monkeypatch `classify()` in tests."
        ) from e
    if not os.path.exists(fp):
        raise FileNotFoundError(
            f"FastText language ID model not found at {fp}. "
            "Download `lid.176.bin` and place it next to this file."
        )
    _model = fasttext.load_model(fp)
    return _model


def classify(text: str) -> bool:
    """Classifies if a text is English or not."""
    if not text:
        return False
    model = _get_model()
    label = model.predict(text)[0][0]
    return label in {"__label__eng_Latn", "__label__en"}


if __name__ == "__main__":
    1+1
