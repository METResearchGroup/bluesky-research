FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/consolidate_enrichment_integrations/ ./pipelines/consolidate_enrichment_integrations/

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/consolidate_enrichment_integrations/helper.py ./services/consolidate_enrichment_integrations/helper.py
COPY services/consolidate_enrichment_integrations/models.py ./services/consolidate_enrichment_integrations/models.py
COPY services/generate_vector_embeddings/models.py ./services/generate_vector_embeddings/models.py
COPY services/ml_inference/models.py ./services/ml_inference/models.py
COPY services/preprocess_raw_data/models.py ./services/preprocess_raw_data/models.py

# copy handler code to /app
COPY pipelines/consolidate_enrichment_integrations/__init__.py /app/__init__.py
COPY pipelines/consolidate_enrichment_integrations/handler.py /app/handler.py

WORKDIR /app/pipelines/consolidate_enrichment_integrations/

# hadolint ignore=DL3003,DL3013,DL3042
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir awslambdaric

WORKDIR /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "awslambdaric"]

CMD ["handler.lambda_handler"]