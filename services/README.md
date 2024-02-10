# Services

This repo contains the code for different microservices that make up our pipeline.

At a high level, our data pipeline consists of the following steps:
1. Sync data from firehose (this lives in `sync/stream`) and write to S3.
2. Filter and preprocess raw data. Determine which data will be passed into
our ranking and recommendation service. This code lives in the `feed_preprocessing/` directory.
3. Ranking and recommendation services. These services will take the filtered posts and (a) rank them according to our algorithms and (b) create the recommended feeds. This code lives in the `recommendation` directory.
4. Additional preprocessing and filtering of posts after ranking and recommendations. After we have our recommended feeds, we do some additional processing and validation. This code lives in the `feed_postprocessing` directory.
5. Saving feeds to database and making them available for Bluesky to query. The `manage_bluesky_feeds` service will manage the process of having up-to-date posts available for our internal API to collect when pinged by
Bluesky.

These microservices will all be managed through a central API service via API Gateway, and the API code lives in the `api` directory.

Any ML training/inference that needs to be done will be managed through the `classify` service.

Orchestration will be handled using [Mage](https://docs.mage.ai/introduction/overview).