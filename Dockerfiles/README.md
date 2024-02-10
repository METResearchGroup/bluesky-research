# Dockerfiles

Each service is hosted as a Docker image.

Steps to replicate:
1. Create repos in ECR for each service. The repo names are "<service>_service" (e.g., "recommendation_service").
![Example of ECR repos](/assets/ecr_repos.png)
2. Log into ECR:
```
aws ecr get-login-password | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query Account --output text).dkr.ecr.YOUR_REGION.amazonaws.com
```
3.
- (Option A) Run the "deploy_all_images.py" script, which redeploys each of the services:
    ```
    python deploy_all_images.py
    ```
- (Option B) Re-deploy a single image manually and then update the lambda (assuming that the lambdas are all named the same as their corresopnding service)
    ```
    docker build -t <service>_service:latest -f ./Dockerfiles/<service>.Dockerfile .
    docker tag <service>_service:latest 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    docker push 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    aws lambda update-function-code --function-name <service> --image-uri 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    ```
