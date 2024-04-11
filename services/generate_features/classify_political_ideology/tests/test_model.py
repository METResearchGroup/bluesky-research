"""Unit tests for model.py"""

import pytest
from services.generate_features.classify_political_ideology.model import classify, classify_post_texts


def test_classify():
    # Test with a post that should be classified as left-leaning
    post = {"uid": "test_uid", "text": "I think that the government should provide more social services."}
    result = classify(post)
    assert result["label"] == "left-leaning"

    # Test with a post that should be classified as right-leaning
    post = {"uid": "test_uid", "text": "I've reached out to the offices of all 22 Democrats who voted to censure Rep. Rashida Tlaib because of her comments on Israel/Palestine to see if they planned to do the same for her Republican colleague from Michigan."}
    result = classify(post)
    assert result["label"] == "right-leaning"

    # Test with a post that should be classified as unclear
    post = {"uid": "test_uid", "text": "This has been the fundamental problem the political press has had with Trump. They've been conditioned to believe it's only a scandal if it's a secret that's been exposed, so all the crimes that Trump does brazenly out in the open — bragging about them on camera even — somehow don't count."}
    result = classify(post)
    assert result["label"] == "unclear"


def test_classify_post_texts():
    # Test with a list of posts that should be classified as left-leaning, right-leaning, and unclear
    post_texts = [
        "I think that the government should provide more social services.",
        "I've reached out to the offices of all 22 Democrats who voted to censure Rep. Rashida Tlaib because of her comments on Israel/Palestine to see if they planned to do the same for her Republican colleague from Michigan.",
        "This has been the fundamental problem the political press has had with Trump. They've been conditioned to believe it's only a scandal if it's a secret that's been exposed, so all the crimes that Trump does brazenly out in the open — bragging about them on camera even — somehow don't count."
    ]
    results = classify_post_texts(post_texts)
    assert len(results) == 3
    assert results[0]["label"] == "left-leaning"
    assert results[1]["label"] == "right-leaning"
    assert results[2]["label"] == "unclear"
