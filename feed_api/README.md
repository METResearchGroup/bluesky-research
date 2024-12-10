# Feed API

This is the API that Bluesky will query in order to get the feeds for a given user.

Related links from Bluesky:
- https://docs.bsky.app/docs/starter-templates/custom-feeds
- https://github.com/bluesky-social/feed-generator

The core FastAPI app is in `app.py`.

This is currently being run on an EC2 instance with `nohup python app.py &`. The logs are sent to Cloudwatch for monitoring.
