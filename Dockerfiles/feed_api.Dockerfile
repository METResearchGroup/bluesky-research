FROM python:3.10-slim

WORKDIR /app

COPY feed_api/* ./feed_api/

# change directory to feed_api
WORKDIR /app/feed_api

RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

ENV PYTHONPATH=/app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "80"]