FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/add_users_to_study/handler.py ./pipelines/add_users_to_study/handler.py
COPY pipelines/add_users_to_study/requirements.txt ./pipelines/add_users_to_study/requirements.txt

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/participant_data/*.py ./services/participant_data/

WORKDIR /app/pipelines/add_users_to_study

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["uvicorn", "handler:app", "--reload"]
