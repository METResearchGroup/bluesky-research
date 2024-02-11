#!/bin/bash

# Builds a single Docker image and runs a container from it locally.

# Check if a service name has been provided
if [ $# -eq 0 ]; then
    echo "No service name provided."
    exit 1
fi

SERVICE_NAME="$1"

# Build the Docker image
docker build -t ${SERVICE_NAME} .

# Check if a container with the same name is already running, and remove it
if [ $(docker ps -a -q -f name=^/${SERVICE_NAME}$) ]; then
    echo "Removing existing container with name: ${SERVICE_NAME}"
    docker rm -f ${SERVICE_NAME}
fi

# Run the Docker container
docker run -d --name ${SERVICE_NAME} ${SERVICE_NAME}

# Wait a bit for the container to start up (optional, adjust sleep time as needed)
sleep 5

# Attach to the Docker container (for interactive services)
# Note: This will fail for non-interactive services that exit immediately after running
if docker ps -a -q -f status=running -f name=^/${SERVICE_NAME}$ > /dev/null; then
    echo "Attaching to running container: ${SERVICE_NAME}. To detach, use Ctrl+P followed by Ctrl+Q."
    docker exec -it ${SERVICE_NAME} bash
else
    echo "Container '${SERVICE_NAME}' is not running. Displaying logs instead:"
    docker logs ${SERVICE_NAME}
fi
