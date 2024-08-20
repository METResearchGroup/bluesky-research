# FROM python:3.10-slim
FROM public.ecr.aws/lambda/python:3.10

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
COPY services/participant_data/* ./services/participant_data/
COPY services/preprocess_raw_data/*.py ./services/preprocess_raw_data/
COPY services/preprocess_raw_data/helper.py ./services/preprocess_raw_data/helper.py
COPY services/preprocess_raw_data/classify_bots/*.py ./services/preprocess_raw_data/classify_bots/
COPY services/preprocess_raw_data/classify_hate_speech/*.py ./services/preprocess_raw_data/classify_hate_speech/
COPY services/preprocess_raw_data/classify_language/*.py ./services/preprocess_raw_data/classify_language/
COPY services/preprocess_raw_data/classify_language/lid.176.bin ./services/preprocess_raw_data/classify_language/lid.176.bin
COPY services/preprocess_raw_data/classify_nsfw_content/*.py ./services/preprocess_raw_data/classify_nsfw_content/
COPY services/preprocess_raw_data/classify_spam/*.py ./services/preprocess_raw_data/classify_spam/
COPY services/preprocess_raw_data/update_bluesky_mute_lists/*.py ./services/preprocess_raw_data/update_bluesky_mute_lists/
COPY services/sync/search/*.py ./services/sync/search/
COPY services/sync/stream/export_data.py ./services/sync/stream/export_data.py
COPY services/sync/most_liked_posts/helper.py ./services/sync/most_liked_posts/helper.py

# copy handler code to /app
COPY pipelines/preprocess_raw_data/__init__.py /app/__init__.py
COPY pipelines/preprocess_raw_data/handler.py /app/handler.py

# install Git and build-essential
# requires yum instead of apt-get (if building on top of lambda base image)
# hadolint ignore=DL3008,DL3015,DL3033
RUN yum -y update \
    && yum groupinstall -y "Development Tools" \
    && yum install -y git \
    && yum clean all

# install fastText from source (pip install isn't working)
# hadolint ignore=DL3003,DL3042
RUN git clone https://github.com/facebookresearch/fastText.git \
    && cd fastText \
    && pip install .

WORKDIR /app/pipelines/preprocess_raw_data/

# hadolint ignore=DL3003,DL3013,DL3042
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir awslambdaric

WORKDIR /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "awslambdaric"]

CMD ["handler.lambda_handler"]
