FROM python:3.10-slim

WORKDIR /app

# add .env env vars to the container
COPY ../.env ./.env
