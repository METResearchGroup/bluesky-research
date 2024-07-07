FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/calculate_superposters/ ./pipelines/calculate_superposters/

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/preprocess_raw_data/models.py ./services/preprocess_raw_data/models.py
COPY services/preprocess_raw_data/export_data.py ./services/preprocess_raw_data/export_data.py
COPY services/calculate_superposters /app/services/calculate_superposters

WORKDIR /app/pipelines/calculate_superposters

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "main.py"]