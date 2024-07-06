FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/preprocess_raw_data/ ./pipelines/preprocess_raw_data/

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY ml_tooling/inference_helpers.py ./ml_tooling/inference_helpers.py

COPY transform/* ./transform/

COPY services/consolidate_post_records/* ./services/consolidate_post_records/
COPY services/preprocess_raw_data/ ./services/preprocess_raw_data/

# install Git and build-essential
# hadolint ignore=DL3008,DL3015
RUN apt-get update \
    && apt-get install -y build-essential git \
    && apt-get install -y g++ \
    && rm -rf /var/lib/apt/lists/*

# install fastText from source (pip install isn't working)
# hadolint ignore=DL3003,DL3042
RUN git clone https://github.com/facebookresearch/fastText.git \
    && cd fastText \
    && pip install .

WORKDIR /app/pipelines/preprocess_raw_data/

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
