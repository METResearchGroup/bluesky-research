#!/bin/bash

# Determine the script's directory
if [ -d "./scripts" ]; then
    SCRIPTS_DIR="./scripts"
else
    SCRIPTS_DIR="."
fi

# Rebuilds the Feed API service. Rebuilds the image and then rebuilds the
# lambda with the new image.
$SCRIPTS_DIR/deploy_image_to_ecr.sh feed_api

# Update the lambda with the new image
aws lambda update-function-code \
    --function-name bsky-api-lambda \
    --image-uri 517478598677.dkr.ecr.us-east-2.amazonaws.com/feed_api_service:latest

# redeploy API Gateway API.
aws apigateway create-deployment \
    --rest-api-id $(aws apigateway get-rest-apis --query "items[?name=='bluesky_feed_api_gateway'].id" --output text) \
    --stage-name prod
