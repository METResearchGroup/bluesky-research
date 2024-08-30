FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/sync_post_records/most_liked ./pipelines/sync_post_records/most_liked

# make the lib/ directory
COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
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

# copy handler code to /app
COPY pipelines/sync_post_records/most_liked/__init__.py /app/__init__.py
COPY pipelines/sync_post_records/most_liked/handler.py /app/handler.py

WORKDIR /app/pipelines/sync_post_records/most_liked

# install packages. Install fasttext from source to avoid dependency hell
# hadolint ignore=DL3003,DL3027,DL3013,DL3042
RUN apt update && apt install -y git g++ \ 
    && pip install --no-cache-dir -r requirements.txt \
    && git clone https://github.com/facebookresearch/fastText.git \
    && cd fastText \
    && pip install . --no-cache-dir \
    && pip install --no-cache-dir awslambdaric

WORKDIR /app

ENV PYTHONPATH=/app
    
ENTRYPOINT ["python", "-m", "awslambdaric"]

CMD ["handler.lambda_handler"]