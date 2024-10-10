import pandas as pd

from services.preprocess_raw_data.classify_language.helper import (
    filter_text_is_english,
    filter_text_is_english_serial,
    filter_text_is_english_parallel,
)

num_samples = 5_000_000
texts = [f"this is an example text with number {i}" for i in range(num_samples)]
pd_texts = pd.Series(texts)

def test_classify_single_post_serial():
    res = filter_text_is_english_serial(texts=pd_texts)
    assert res.all()


def test_classify_single_post_parallel():
    res = filter_text_is_english_parallel(texts=pd_texts)
    assert res.all()


def test_classify_single_post():
    res = filter_text_is_english(texts=pd_texts)
    assert res.all()
