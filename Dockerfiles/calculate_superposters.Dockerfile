FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env

COPY pipelines/calculate_superposters/*.py ./pipelines/calculate_superposters/
COPY pipelines/calculate_superposters/requirements.txt ./pipelines/calculate_superposters/

COPY lib/aws/*.py ./lib/aws/
COPY lib/constants.py ./lib/constants.py
COPY lib/db/bluesky_models/* ./lib/db/bluesky_models/
COPY lib/db/manage_local_data.py ./lib/db/manage_local_data.py
COPY lib/helper.py ./lib/helper.py
COPY lib/log/logger.py ./lib/log/logger.py

COPY services/calculate_superposters/helper.py ./services/calculate_superposters/helper.py

# copy handler code to /app
COPY pipelines/calculate_superposters/__init__.py /app/__init__.py
COPY pipelines/calculate_superposters/handler.py /app/handler.py

WORKDIR /app/pipelines/calculate_superposters

# hadolint ignore=DL3003,DL3013,DL3042
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir awslambdaric

WORKDIR /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "awslambdaric"]

CMD ["handler.lambda_handler"]
