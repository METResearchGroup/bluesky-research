FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/sync_post_records/firehose ./pipelines/sync_post_records/firehose

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY transform/* ./transform/

COPY services/participant_data/study_users.py ./services/participant_data/study_users.py
COPY services/sync/stream/* ./services/sync/stream/
COPY services/consolidate_post_records/* ./services/consolidate_post_records/
COPY services/participant_data/helper.py ./services/participant_data/helper.py
COPY services/participant_data/mock_users.py ./services/participant_data/mock_users.py
COPY services/participant_data/models.py ./services/participant_data/models.py
COPY services/participant_data/study_users.py ./services/participant_data/study_users.py

WORKDIR /app/pipelines/sync_post_records/firehose

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install awscli==1.33.38

ENV PYTHONPATH=/app

CMD ["python", "firehose.py"]