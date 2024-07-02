FROM python:3.10-slim

WORKDIR /app

COPY pipelines/sync_post_records/most_liked ./pipelines/sync_post_records/most_liked

COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/mongodb.py ./lib/db/mongodb.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/preprocess_raw_data/classify_language/*.py ./services/preprocess_raw_data/classify_language/
COPY services/preprocess_raw_data/classify_language/lid.176.bin ./services/preprocess_raw_data/classify_language/lid.176.bin
COPY services/sync/search/helper.py ./services/sync/search/helper.py
COPY services/sync/most_liked_posts/helper.py ./services/sync/most_liked_posts/

COPY transform/* ./transform/

WORKDIR /app/pipelines/sync_post_records/most_liked

# install Git and latest cpp compiler
RUN apt-get update \
    && apt-get install -y build-essential git \
    && apt-get install -y g++ \
    && rm -rf /var/lib/apt/lists/*

# install fastText from source (pip install isn't working)
# RUN git clone https://github.com/facebookresearch/fastText.git \
#     && cd fastText \
#     && pip install .

RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

ENV PYTHONPATH=/apps

CMD ["python", "most_liked.py"]