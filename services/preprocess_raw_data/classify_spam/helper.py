"""Classifies whether a post contains possible spam.

Spam classification can be done in many ways.

Here, we're just using content-based heuristics for now.
"""

import pandas as pd

from lib.helper import track_performance

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


@track_performance
def filter_posts_have_spam(texts: pd.Series) -> pd.Series:
    """Filters posts that have spam."""
    shortened_urls_check = texts.isin(url_shorteners)
    too_many_hashtags_check = texts.str.count("#") > HASHTAG_MAX_LIMIT
    spam_words_check = texts.isin(spam_words)
    return shortened_urls_check | too_many_hashtags_check | spam_words_check
