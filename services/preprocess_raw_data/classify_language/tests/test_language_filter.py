import os

import pandas as pd
import pytest

from services.preprocess_raw_data.classify_language import helper as lang_helper


def always_true(_: str) -> bool:
    return True


def always_false(_: str) -> bool:
    return False


def test_filter_text_is_english_serial_monkeypatched_classifier(monkeypatch):
    # Avoid requiring the FastText model artifact in unit tests.
    monkeypatch.setattr(lang_helper, "classify", always_true)
    pd_texts = pd.Series([f"this is an example text with number {i}" for i in range(10)])
    res = lang_helper.filter_text_is_english_serial(texts=pd_texts)
    assert res.all()


@pytest.mark.skipif(
    os.environ.get("RUN_LANGUAGE_CLASSIFIER_PARALLEL_TESTS") != "1",
    reason="Parallel classifier tests are opt-in (can be flaky/slow in CI).",
)
def test_filter_text_is_english_parallel_monkeypatched_classifier(monkeypatch):
    # The function we monkeypatch must be picklable for multiprocessing.
    monkeypatch.setattr(lang_helper, "classify", always_true)
    pd_texts = pd.Series([f"this is an example text with number {i}" for i in range(100)])
    res = lang_helper.filter_text_is_english_parallel(texts=pd_texts)
    assert res.all()


def test_filter_text_is_english_default_mode_serial(monkeypatch):
    monkeypatch.setattr(lang_helper, "classify", always_true)
    pd_texts = pd.Series(["hello", "world", "test"])
    res = lang_helper.filter_text_is_english(texts=pd_texts, mode="serial")
    assert res.all()


def test_record_and_post_language_do_not_trust_upstream_langs(monkeypatch):
    monkeypatch.setattr(lang_helper, "classify", always_false)

    class FakeRecord:
        def __init__(self, text: str, langs: str | None):
            self.text = text
            self.langs = langs

    class FakePost:
        def __init__(self, uri: str, text: str, langs: str | None):
            self.uri = uri
            self.text = text
            self.langs = langs

    # Even if upstream says "en", we should still run our classifier and return False.
    assert lang_helper.record_is_english(FakeRecord(text="hola mundo", langs="en")) is False
    assert lang_helper.classify_single_post(
        FakePost(uri="at://did:plc:abc/app.bsky.feed.post/123", text="hola mundo", langs="en")
    ) == {"uri": "at://did:plc:abc/app.bsky.feed.post/123", "is_english": False}
