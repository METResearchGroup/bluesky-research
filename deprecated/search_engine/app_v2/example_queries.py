# Example queries for the Bluesky Data Access App Demo
# Each query is a dict with a human-readable name and a filter dict

EXAMPLE_QUERIES = [
    {
        "name": "Recent Climate Hashtag Posts",
        "filters": {
            "Temporal": {"date_range": "2024-06-01 to 2024-06-10"},
            "Content": {"hashtags": ["#climate"]},
        },
    },
    {
        "name": "Positive Sentiment, Political, Left-Leaning",
        "filters": {
            "Sentiment": {"valence": "positive"},
            "Political": {"political": "Yes", "slant": "left"},
        },
    },
    {
        "name": "Toxic Election Posts",
        "filters": {
            "Content": {"hashtags": ["#election"]},
            "Sentiment": {"toxicity": "Toxic"},
        },
    },
    {
        "name": "Posts by User 'alice@bsky.social' About Climate",
        "filters": {
            "User": {"handles": ["alice@bsky.social"]},
            "Content": {"keywords": ["climate"]},
        },
    },
    {
        "name": "Right-Slant, Negative Valence, Political",
        "filters": {
            "Sentiment": {"valence": "negative"},
            "Political": {"political": "Yes", "slant": "right"},
        },
    },
]
