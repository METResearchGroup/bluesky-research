FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/classify_records/perspective_api/ ./pipelines/classify_records/perspective_api/
COPY pipelines/classify_records/helper.py ./pipelines/classify_records/helper.py

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY ml_tooling/inference_helpers.py ./ml_tooling/inference_helpers.py
COPY ml_tooling/perspective_api/ ./ml_tooling/perspective_api/

COPY services/ml_inference/models.py ./services/ml_inference/models.py
COPY services/ml_inference/perspective_api/ ./services/ml_inference/perspective_api/
COPY services/preprocess_raw_data/export_data.py ./services/preprocess_raw_data/export_data.py
COPY services/preprocess_raw_data/models.py ./services/preprocess_raw_data/models.py

WORKDIR /app/pipelines/classify_records/perspective_api/

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "perspective_api.py"]