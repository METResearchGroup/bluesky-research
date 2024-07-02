FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/sync_post_records/firehose ./pipelines/sync_post_records/firehose

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
# TODO: DB will be removed when we store state externally.
COPY lib/db/sql/sync_database.py ./lib/db/sql/sync_database.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY transform/* ./transform/

COPY services/sync/stream/* ./services/sync/stream/

WORKDIR /app/pipelines/sync_post_records/firehose

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "firehose.py"]