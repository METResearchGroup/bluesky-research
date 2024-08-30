#!/bin/bash

# Determine the script's directory
if [ -d "./scripts" ]; then
    SCRIPTS_DIR="./scripts"
else
    SCRIPTS_DIR="."
fi

# Rebuilds the service. Rebuilds the image and then rebuilds the
# lambda with the new image.
$SCRIPTS_DIR/deploy_image_to_ecr.sh $1

# Update the lambda with the new image
aws lambda update-function-code \
    --function-name ${1}_lambda \
    --image-uri 517478598677.dkr.ecr.us-east-2.amazonaws.com/${1}_service:latest
