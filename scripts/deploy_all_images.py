"""Deploys all Docker images to ECR and rebuilds their respective lambdas."""
import boto3
import subprocess


# services: ECR repo name
service_to_ecr_repo_map = {
    "api": "api-service",
    "classify": "classify-service",
    "data_pipeline_orchestration": "data-pipeline-orchestration-service",
    "feed_postprocessing": "feed-postprocessing-service",
    "feed_preprocessing": "feed-preprocessing-service",
    "manage_bluesky_feeds": "manage-bluesky-feeds-service",
    "manage_users": "manage-users-service",
    "recommendation": "recommendation-service",
    "sync": "sync-service"
}

latest_tag = "latest"

ecr_client = boto3.client('ecr')
lambda_client = boto3.client('lambda')

def build_and_push_image(service_name, ecr_repo_name):
    # Authenticate Docker to ECR
    auth_response = ecr_client.get_authorization_token()
    token = auth_response['authorizationData'][0]['authorizationToken']
    ecr_url = auth_response['authorizationData'][0]['proxyEndpoint']
    subprocess.run(['docker', 'login', '-u', 'AWS', '-p', token, ecr_url], check=True)

    # Build the Docker image
    subprocess.run(['docker', 'build', '-t', f'{ecr_repo_name}:{latest_tag}', f'./Dockerfiles/{service_name}.Dockerfile'], check=True) # noqa

    # Tag the Docker image
    image_uri = f'{ecr_url}/{ecr_repo_name}:{latest_tag}'
    subprocess.run(['docker', 'tag', f'{ecr_repo_name}:{latest_tag}', image_uri], check=True)

    # Push the Docker image to ECR
    subprocess.run(['docker', 'push', image_uri], check=True)
    return image_uri

def update_lambda_function(function_name, image_uri):
    lambda_client.update_function_code(
        FunctionName=function_name,
        ImageUri=image_uri,
        Publish=True
    )

for service, repo in service_to_ecr_repo_map.items():
    print(f'Processing {service}')
    image_uri = build_and_push_image(service, repo)
    update_lambda_function(service, image_uri)
    print(f'{service} updated successfully')
