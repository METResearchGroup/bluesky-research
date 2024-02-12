from transform.preprocessing.filters import (
    author_is_not_blocked,
    example_filtering_function,
    filter_nonenglish_posts,
    has_no_explicit_content,
    is_in_network,
    is_within_similar_networks,
    post_is_recent
)
from transform.preprocessing.preprocessing import (
    example_preprocessing_function, example_enrichment_function
)


filtering_pipeline: list[callable] = [
    author_is_not_blocked,
    example_filtering_function,
    filter_nonenglish_posts,
    has_no_explicit_content,
    is_in_network,
    is_within_similar_networks,
    post_is_recent
]
feed_preprocessing_pipeline: list[callable] = [
    example_preprocessing_function, example_enrichment_function
]
