# Pipelines

Maintains the separate pipeline logic for each step.

The steps, as planned so far, will look something like this:
- Get raw posts from Bluesky firehose.
- Given the latest raw posts, filter the posts.
- Given the latest filtered posts, generate features.
- ML-specific:
    - Each ML service can expose its own API and manage its own database and (possibly) generate features offline.
    - Each ML service can manage its own retraining schedule (if needed).
- Given the latest posts with features, generate embeddings from the candidate generation model and add to the vector database. Rerun the vector database logic to re-cluster posts.
- Given the latest candidates in the vector database, generate up-to-date candidates per user. Then, generate feeds per user and store in the database and cache.
- Other pipelines:
    - Update latest context.
    - Update latest user engagement data.
    - Update latest mutelists.
    - Update other integrations + syncs (to be added, TBD).
    - etc.

The actual functionality will be handled at the service level, but the pipelines will call the services and manage higher-level things such as orchestration. Each of these can run separately and store their results in a database for downstream services to consume.
