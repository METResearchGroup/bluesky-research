FROM public.ecr.aws/lambda/python:3.10

# Set the working directory to /var/task
WORKDIR /var/task

# Copy the function code
COPY pipelines/sync_post_records/most_liked ./pipelines/sync_post_records/most_liked

# Copy necessary files and directories
COPY ../.env ./.env
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
COPY pipelines/sync_post_records/most_liked/most_liked.py ./most_liked.py
COPY transform/* ./transform/

# Install dependencies
COPY pipelines/sync_post_records/most_liked/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the CMD to your handler
CMD ["most_liked.lambda_handler"]