FROM python:3.10-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./lib/ /app/lib/
COPY ./services/feed_preprocessing/ /app/services/feed_preprocessing/

WORKDIR /app/

ENV PYTHONPATH "${PYTHONPATH}:/app"

# Command to run the handler.py file
CMD ["python", "services/feed_preprocessing/handler.py"]
