# FROM python:3.10-slim
FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

COPY feed_api/* /app/feed_api/
COPY lib/helper.py /app/lib/helper.py
COPY lib/aws/*.py /app/lib/aws/
COPY lib/constants.py /app/lib/constants.py
COPY lib/db/bluesky_models/*.py /app/lib/db/bluesky_models/
COPY services/sync/search/helper.py /app/services/sync/search/helper.py
COPY services/participant_data/helper.py /app/services/participant_data/helper.py
COPY services/participant_data/mock_users.py /app/services/participant_data/mock_users.py
COPY services/participant_data/models.py /app/services/participant_data/models.py
COPY transform/bluesky_helper.py /app/transform/bluesky_helper.py
COPY transform/transform_raw_data.py /app/transform/transform_raw_data.py

# copy app.py to root directory.
COPY feed_api/__init__.py /app/__init__.py
COPY feed_api/app.py /app/app.py

# change directory to feed_api
WORKDIR /app/feed_api

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD [ "app.handler" ]
