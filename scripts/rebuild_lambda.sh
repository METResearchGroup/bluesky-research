#!/bin/bash

# Determine the script's directory
if [ -d "./scripts" ]; then
    SCRIPTS_DIR="./scripts"
else
    SCRIPTS_DIR="."
fi

VALID_LAMBDAS=("calculate_superposters" "ml_inference_sociopolitical" "ml_inference_perspective_api" "compact_dedupe_data" "consolidate_enrichment_integrations" "generate_vector_embeddings" "preprocess_raw_data" "sync_most_liked_feed" "rank_score_feeds")

if [ "$1" == "all" ]; then
    for lambda in "${VALID_LAMBDAS[@]}"; do
        $SCRIPTS_DIR/deploy_image_to_ecr.sh "$lambda"
        aws lambda update-function-code \
            --function-name "${lambda}_lambda" \
            --image-uri 517478598677.dkr.ecr.us-east-2.amazonaws.com/"${lambda}_service:latest"
    done
else
    if [[ " ${VALID_LAMBDAS[@]} " =~ " $1 " ]]; then
        $SCRIPTS_DIR/deploy_image_to_ecr.sh "$1"
        aws lambda update-function-code \
            --function-name "${1}_lambda" \
            --image-uri 517478598677.dkr.ecr.us-east-2.amazonaws.com/"${1}_service:latest"
    else
        echo "Error: Invalid argument '$1'. Valid arguments are: ${VALID_LAMBDAS[*]}"
        exit 1
    fi
fi
