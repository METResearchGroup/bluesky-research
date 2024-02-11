#!/bin/bash

# Builds and deploys Docker images to the AWS ECR repository

# Function to deploy a single service
deploy_service() {
    local service="$1"
    local aws_account_id=$(aws sts get-caller-identity --query Account --output text)
    local region="us-east-2"
    local repository_url="${aws_account_id}.dkr.ecr.${region}.amazonaws.com"

    # Authenticate Docker to the AWS ECR
    aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${repository_url}

    # Build the Docker image
    docker build -t ${service}:latest -f ./Dockerfiles/${service}.Dockerfile . || { echo "Build failed for ${service}"; return 1; }

    # Tag the Docker image for the ECR repository
    docker tag ${service}:latest ${repository_url}/${service}_service:latest || { echo "Tagging failed for ${service}"; return 1; }

    # Push the Docker image to the ECR repository
    docker push ${repository_url}/${service}_service:latest || { echo "Push failed for ${service}"; return 1; }

    echo "Deployment of ${service} completed successfully."
}

# Check if a service name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <service-name|all>"
    exit 1
fi

if [ "$1" = "all" ]; then
    # Loop through all Dockerfiles and deploy each service
    for dockerfile in ./Dockerfiles/*.Dockerfile; do
        service=$(basename "$dockerfile" .Dockerfile)
        echo "Deploying service: $service"
        deploy_service "$service" || { echo "Deployment failed for ${service}"; exit 1; }
    done
else
    # Deploy a specific service
    deploy_service "$1"
fi
