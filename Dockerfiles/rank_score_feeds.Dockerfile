FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/rank_score_feeds/ ./pipelines/rank_score_feeds/

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/calculate_superposters/helper.py ./services/calculate_superposters/helper.py
COPY services/calculate_superposters/models.py ./services/calculate_superposters/models.py
COPY services/consolidate_enrichment_integrations/models.py ./services/consolidate_enrichment_integrations/models.py
COPY services/participant_data/helper.py ./services/participant_data/helper.py
COPY services/participant_data/mock_users.py ./services/participant_data/mock_users.py
COPY services/participant_data/models.py ./services/participant_data/models.py
COPY services/preprocess_raw_data/models.py ./services/preprocess_raw_data/models.py
COPY services/rank_score_feeds/helper.py ./services/rank_score_feeds/helper.py
COPY services/rank_score_feeds/models.py ./services/rank_score_feeds/models.py

# copy handler code to /app
COPY pipelines/rank_score_feeds/__init__.py /app/__init__.py
COPY pipelines/rank_score_feeds/handler.py /app/handler.py

WORKDIR /app/pipelines/rank_score_feeds/

# hadolint ignore=DL3003,DL3013,DL3042
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir awslambdaric

WORKDIR /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "awslambdaric"]

CMD ["handler.lambda_handler"]
