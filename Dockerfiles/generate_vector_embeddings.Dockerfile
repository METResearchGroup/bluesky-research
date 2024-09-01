FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/generate_vector_embeddings/ ./pipelines/generate_vector_embeddings/

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/generate_vector_embeddings/helper.py ./services/generate_vector_embeddings/helper.py

# copy handler code to /app
COPY pipelines/generate_vector_embeddings/__init__.py /app/__init__.py
COPY pipelines/generate_vector_embeddings/handler.py /app/handler.py

WORKDIR /app/pipelines/generate_vector_embeddings/

# hadolint ignore=DL3003,DL3013,DL3042
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir awslambdaric

WORKDIR /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "awslambdaric"]

CMD ["handler.lambda_handler"]