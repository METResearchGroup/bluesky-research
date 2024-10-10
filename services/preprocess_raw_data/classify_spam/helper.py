"""Classifies whether a post contains possible spam.

Spam classification can be done in many ways.

Here, we're just using content-based heuristics for now.
"""

import pandas as pd

url_shorteners = set(
    [
        "bit.ly",
        "tinyurl.com",
        "goo.gl",
        "t.co",
        "ow.ly",
        "is.gd",
        "buff.ly",
        "adf.ly",
        "t.me",
        "s.shopee.com",
    ]
)

HASHTAG_MAX_LIMIT = 3

spam_words = set(
    [
        "click",
        "purchase",
        "order",
        "subscribe",
        "bonus",
        "offer",
        "sale",
        "discount",
        "cheap",
        "promo",
        "guarantee",
        "risk-free",
        "money-back",
        "refund",
        "prize",
        "cash",
        "reward",
        "gift",
        "voucher",
        "coupon",
        "best price",
        "act now",
        "don't miss",
        "apply now",
        "join now",
        "register",
        "membership",
        "click here",
    ]
)


def check_for_shortened_urls_in_text(text: str) -> bool:
    """Checks for shortened URLs in the text."""
    return any(
        any(shortener in text for shortener in url_shorteners) for text in text.split()
    )


def check_for_too_many_hashtags(text: str) -> bool:
    """Checks for too many hashtags in the text."""
    hashtags = text.count("#")
    return hashtags > HASHTAG_MAX_LIMIT


def check_for_spam_words_in_text(text: str) -> bool:
    """Checks for spam words in the text."""
    return any(word in text for word in spam_words)


def filter_posts_have_spam(texts: pd.Series) -> pd.Series:
    """Filters posts that have spam."""
    shortened_urls_check = texts.apply(check_for_shortened_urls_in_text)
    too_many_hashtags_check = texts.apply(check_for_too_many_hashtags)
    spam_words_check = texts.apply(check_for_spam_words_in_text)
    return shortened_urls_check | too_many_hashtags_check | spam_words_check
