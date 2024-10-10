import pandas as pd

from services.preprocess_raw_data.classify_spam.helper import (
    filter_posts_have_spam, url_shorteners, spam_words
)

posts_with_shortened_urls = pd.Series([url for url in url_shorteners])
posts_with_too_many_hashtags = pd.Series(["###", "####", "#####"])
posts_with_spam_words = pd.Series([word for word in spam_words])

posts_with_spam = pd.concat([posts_with_shortened_urls, posts_with_too_many_hashtags, posts_with_spam_words])

posts_without_spam = pd.Series([
    "This is a test post",
    "Hello, world!",
    "This is a test post",
    "Hello, world!",
    "This is a test post",
    "Hello, world!",
    "This is a test post",
    "Hello, world!",
])

spam_test_posts = pd.concat([posts_with_spam, posts_without_spam])

def test_spam_posts_filtered():
    filtered_posts = filter_posts_have_spam(spam_test_posts)
    assert sum(filtered_posts) == len(posts_with_spam)


def test_spam_posts_not_filtered():
    filtered_posts = filter_posts_have_spam(posts_without_spam)
    assert sum(filtered_posts) == 0


def test_posts_for_spam():
    filtered_posts = filter_posts_have_spam(spam_test_posts)
    assert sum(filtered_posts) == len(posts_with_spam)
