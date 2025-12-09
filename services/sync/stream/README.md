# Firehose sync service
Interacts with Bluesky firehose to get posts.

Inspired by the following:
- https://github.com/MarshalX/bluesky-feed-generator/tree/main/server
- https://github.com/bluesky-astronomy/astronomy-feeds/tree/main/server

Main Bluesky team feed overview: https://github.com/bluesky-social/feed-generator#overview

We expect to handle something on the order of ~1M posts/day.

We're starting with using [Peewee](https://pypi.org/project/peewee/) as a simple ORM layer for now (especially since there's out-of-the-box examples for this) and then will migrate to [Django](https://www.djangoproject.com/) later on. The migration should be a simple process since we're storing everything in a SQLite database for now.

To run the firehose, run:

```python
python app.py
```

This will write the posts to the `firehose.db` database. Expect a 1:5 ratio between firehose events and new post writes. By default, the app will run for 25,000 firehose events, which should lead to about ~5,000 new posts.

We output to local storage in the following format:

```markdown
cache/
├── create/
│   ├── post/
│   ├── like/
│   └── follow/
├── delete/
│   ├── post/
│   ├── like/
│   └── follow/
├── study_user_activity/
│   ├── create/
│   │   ├── post/
│   │   ├── like/
│   │   ├── follow/
│   │   │   ├── follower/
│   │   │   └── followee/
│   │   ├── like_on_user_post/
│   │   └── reply_to_user_post/
│   └── delete/
│       ├── post/
│       └── like/
└── in_network_user_activity/
    └── create/
        └── post/
            └── {author_did}/
```

We have a subsequent job, in `pipelines/sync_post_records/firehose/run_firehose_writes.py`, to manage the process of exporting those to persistent local parquet storage.
