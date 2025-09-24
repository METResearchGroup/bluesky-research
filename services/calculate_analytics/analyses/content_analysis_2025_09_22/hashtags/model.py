import re

CANONICAL_HASHTAG_REPRESENTATIONS = {
    "joebiden": "biden",
    "donald": "trump",
    "donaldtrump": "trump",
    "kamala": "harris",
    "kamalaharris": "harris",
    "elizabethwarren": "warren",
    "rondesantis": "desantis",
    "ericadams": "adams",
    "anthonyblinken": "blinken",
    "tedcruz": "cruz",
    "gavinnewsom": "newsom",
}


def canonicalize_text(text: str) -> str:
    """Returns a canonicalized version of text for hashtag analysis."""
    for (
        canonical_representation,
        canonical_hashtag,
    ) in CANONICAL_HASHTAG_REPRESENTATIONS.items():
        text = text.replace(canonical_representation, canonical_hashtag)
    return text


def preprocess_text(text: str) -> str:
    """Returns a preprocessed version of text for hashtag analysis."""
    lowercased_text = text.lower()
    canonicalized_text = canonicalize_text(lowercased_text)
    return canonicalized_text


def process_hashtags_for_post(post: str) -> dict:
    preprocessed_text = preprocess_text(post)
    hashtags = re.findall(r"#(\w+)", preprocessed_text)
    hashtag_to_count = {}
    for hashtag in hashtags:
        hashtag_to_count[hashtag] = hashtag_to_count.get(hashtag, 0) + 1
    return hashtag_to_count


def analyze_hashtags_for_posts(uri_to_text: dict[str, str]) -> dict[str, dict]:
    """Analyzes hashtags for posts."""
    uri_to_hashtags = {}
    for uri, text in uri_to_text.items():
        uri_to_hashtags[uri] = process_hashtags_for_post(text)
    return uri_to_hashtags
