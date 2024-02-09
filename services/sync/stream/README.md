# Firehose sync service
Interacts with Bluesky firehose to get posts.

Inspired by the following:
- https://github.com/MarshalX/bluesky-feed-generator/tree/main/server
- https://github.com/bluesky-astronomy/astronomy-feeds/tree/main/server

Main Bluesky team feed overview: https://github.com/bluesky-social/feed-generator#overview

From some preliminary testing, it looks like in a random 5 minute window (sampled
on Friday, February 9th, 11am Eastern), we collected ~2600 posts (~60KB). If we extrapolate, then:
- In 10 minutes, we’d have ~5,200 posts (120 KB)
- In 1 hr., we’d have ~31,200 posts (720 KB, 0.72 MB)
- In 24 hrs., we’d have ~750,000 posts (~18 MB)
- In 30 days, we’d have ~23,000,000 posts (540 MB, 0.54 GB)

Takeaways:

- Data storage won’t be an issue, in a month’s worth of sync data from the firehose, I estimate we’d have <1 GB.
- The trouble would be figuring out how to filter our data so we don’t have to consider as much when doing inference for our classification algorithms.