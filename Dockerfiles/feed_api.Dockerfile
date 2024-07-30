# FROM python:3.10-slim
FROM public.ecr.aws/lambda/python:3.10

WORKDIR /app

COPY feed_api/* /app/feed_api/
COPY lib/helper.py /app/lib/helper.py
COPY lib/aws/*.py /app/lib/aws/

# copy app.py to root directory.
COPY feed_api/__init__.py /app/__init__.py
COPY feed_api/app.py /app/app.py

# change directory to feed_api
WORKDIR /app/feed_api

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD [ "app.handler" ]
