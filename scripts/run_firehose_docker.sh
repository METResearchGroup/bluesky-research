#!/bin/bash

# Runs the Docker container for the firehose sync service.

# Will eventually be ported over to Singularity.

# Run the Docker container and mount the AWS credentials
docker run -v ~/.aws:/root/.aws sync_firehose_stream