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

from dataclasses import dataclass, field
import os
from threading import Lock
from typing import Protocol

current_file_directory = os.path.dirname(os.path.abspath(__file__))
binary_filename = "lid.176.bin"
fp = os.path.join(current_file_directory, binary_filename)


class FastTextModelProtocol(Protocol):
    def predict(self, text: str):  # pragma: no cover - protocol only
        ...


@dataclass
class LanguageClassifier:
    """Lazy FastText classifier with explicit lifecycle."""

    model_path: str = fp
    _model: FastTextModelProtocol | None = None
    _lock: Lock = field(default_factory=Lock, repr=False, compare=False)

    def load(self) -> FastTextModelProtocol:
        if self._model is not None:
            return self._model

        with self._lock:
            if self._model is not None:
                return self._model

            try:
                import fasttext  # type: ignore
            except ModuleNotFoundError as e:  # pragma: no cover
                raise ModuleNotFoundError(
                    "fasttext is required for language classification. "
                    "Install it (e.g. from `services/preprocess_raw_data/classify_language/requirements.txt`) "
                    "or monkeypatch `classify()` in tests."
                ) from e

            if not os.path.exists(self.model_path):
                raise FileNotFoundError(
                    f"FastText language ID model not found at {self.model_path}. "
                    "Download `lid.176.bin` and place it next to this file."
                )
            self._model = fasttext.load_model(self.model_path)
            return self._model

    def clear_cache(self) -> None:
        self._model = None


_language_classifier = LanguageClassifier()


def _get_model(classifier: LanguageClassifier | None = None):
    active_classifier = classifier or _language_classifier
    return active_classifier.load()


def classify(text: str, classifier: LanguageClassifier | None = None) -> bool:
    """Classifies if a text is English or not."""
    if not text:
        return False
    model = _get_model(classifier=classifier)
    label = model.predict(text)[0][0]
    return label in {"__label__eng_Latn", "__label__en"}


def clear_classification_cache() -> None:
    """Reset the shared classifier cache for tests or reloads."""
    _language_classifier.clear_cache()


if __name__ == "__main__":
    1 + 1
