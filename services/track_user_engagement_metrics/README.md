# Track user engagement metrics

In this service, we'll track user engagement metrics. The broad categories
of per-user metrics that we'll track are:
- User-level usage metrics (e.g., how long were they on the app?)
- User-level exposure metrics (e.g., which posts did they see and for how long?)
- Aggregate metrics
    - Number of posts that a user has seen
    - Number of posts that a user has engaged with
    - Number of posts that a user has engaged with that are written by authors who are in their network
    - Number of civic posts that a user has engaged with
    - Number of likes that a user has given
    - Number of comments that a user has written
    - Number of posts that a user has written
- Aggregate sliding window metrics: same metrics as aggregate metrics, but across a sliding window (3 days? 1 week?)
- Network metrics:
    - Follower count
    - Number of new posts written by those in the user’s network
