#!/bin/bash

# Runs the Docker container for the firehose sync service.

# Will eventually be ported over to Singularity.

# Run the Docker container and mount the AWS credentials
image_name=$1
if [ -z "$image_name" ]; then
    echo "No image name provided. Using default image name 'sync_firehose_stream'."
    image_name="sync_firehose_stream"
fi

docker run -v ~/.aws:/root/.aws -v ~/.aws/sso/cache:/root/.aws/sso/cache $image_name
