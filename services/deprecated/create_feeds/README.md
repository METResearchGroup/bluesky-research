# Create feeds

This service will create the actual feeds to serve to users.
It will do the following steps:
- Rank and score candidates in order to create feeds.
- Postprocess feeds to make sure that they are what we would like.
- Store the feeds in both a database (so we know what feeds we recommended
for users) and a cache (so Bluesky can get the latest feed recommendations
for a given user).
