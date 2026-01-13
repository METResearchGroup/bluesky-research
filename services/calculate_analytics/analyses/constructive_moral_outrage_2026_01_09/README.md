# Investigating the relationship between constructive and moral outrage content

We're interested in looking at the relationship between the constructiveness of posts and the moral outrage. We found a pattern where there are some posts that appear to have both constructive content and moral outrage. It seems to be posts where people are stating facts but are clearly very upset. This makes actually a decent proportion of all constructive posts, around 10%, and so we're interested in re-evaluating doing our analyses on constructive posts but eliminating posts that also have moral outrage.

Our data is stored in S3. We use a SQL query (`get_per_feed_average_constructiveness.sql`) to grab posts from feeds and to get the average constructiveness of all feeds. Then we use a Python script (`calculate_per_user_per_day_average_constructiveness.py`) to be able to take that and transform it into a per-user, per-day average of the constructiveness of posts shown in that user's feeds.

We then do some follow-up analyses in `r_analyses`, replicating our previous results but with this new subset of data.
