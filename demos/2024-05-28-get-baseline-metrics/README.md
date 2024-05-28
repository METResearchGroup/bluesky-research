# Get baseline metrics

We want to get baseline metrics across both the (1) firehose and (2) most liked feeds.

Specifically, we want to get the following, as a start:
- Distribution of probabilities of toxicity and constructiveness, both across most liked feed and also regular firehose posts (I can do the current subset as a sample, and then expand to a larger pilot later?).
- Sociopolitical and political ideology classification (via LLMs) .
- Facets of combinations of these traits (e.g., toxicity by political lean, etc.).

We do our processing off posts that have already passed preprocessing.
