FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/sync_post_records/most_liked ./pipelines/sync_post_records/most_liked

# make the lib/ directory
COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/mongodb.py ./lib/db/mongodb.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY ml_tooling/inference_helpers.py ./ml_tooling/inference_helpers.py

COPY services/consolidate_post_records/* ./services/consolidate_post_records/
COPY services/preprocess_raw_data/classify_language/*.py ./services/preprocess_raw_data/classify_language/
COPY services/preprocess_raw_data/classify_language/lid.176.bin ./services/preprocess_raw_data/classify_language/lid.176.bin
COPY services/sync/search/helper.py ./services/sync/search/helper.py
COPY services/sync/most_liked_posts/helper.py ./services/sync/most_liked_posts/

COPY transform/* ./transform/

WORKDIR /app/pipelines/sync_post_records/most_liked

RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

ENV PYTHONPATH=/app

CMD ["python", "most_liked.py"]