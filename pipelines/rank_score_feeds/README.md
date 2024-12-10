# Rank Score Feeds

This pipeline generates feeds for users. It takes posts from the latest "n" days (currently 3 days) and ranks/scores them based on our scoring algorithm. It then exports the feeds as .jsonl files to S3, to be read by the app side.

This pipeline is run via `orchestration/recommendation_pipeline.py`.
