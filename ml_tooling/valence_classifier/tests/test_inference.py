import pytest
from ml_tooling.valence_classifier.inference import run_vader_on_posts

def test_run_vader_on_posts_positive():
    posts = [{"uri": "1", "text": "I love this!"}]
    df = run_vader_on_posts(posts)
    assert df.iloc[0]["valence_label"] == "positive"

def test_run_vader_on_posts_negative():
    posts = [{"uri": "2", "text": "I hate this!"}]
    df = run_vader_on_posts(posts)
    assert df.iloc[0]["valence_label"] == "negative"

def test_run_vader_on_posts_neutral():
    posts = [{"uri": "3", "text": "This is a post."}]
    df = run_vader_on_posts(posts)
    assert df.iloc[0]["valence_label"] == "neutral"

def test_run_vader_on_posts_empty_text():
    posts = [{"uri": "4", "text": ""}]
    df = run_vader_on_posts(posts)
    assert df.iloc[0]["valence_label"] == "neutral"

def test_run_vader_on_posts_missing_text():
    posts = [{"uri": "5"}]
    df = run_vader_on_posts(posts)
    assert df.iloc[0]["valence_label"] == "neutral" 