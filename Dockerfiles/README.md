# Dockerfiles

Each service is hosted as a Docker image.

Steps to replicate:
1. Create repos in ECR for each service. The repo names are "<service>_service" (e.g., "recommendation_service").
![Example of ECR repos](/assets/ecr_repos.png)
2. Log into ECR:
```
aws ecr get-login-password --region us-east-2 --profile <profile>| docker login --username AWS --password-stdin $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-2.amazonaws.com
```
Make sure that you've authenticated first, i.e., `aws sso login --profile <profile>`. If this doesn't work, make sure that you've (1) authenticated and (2) set `AWS_PROFILE` as an env var in your `.zshrc` or `.bash_profile` (e.g., `export AWS_PROFILE=<profile>`).

```
aws ecr get-login-password | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-2.amazonaws.com
```

A successful login should print "Login Succeeded".
3.
- (Option A) Run the "deploy_all_images.py" script, which redeploys each of the services:
    ```
    python deploy_all_images.py
    ```
- (Option B) Re-deploy a single image manually and then update the lambda (assuming that the lambdas are all named the same as their corresopnding service)
    ```
    docker build -t <service>:latest -f ./Dockerfiles/<service>.Dockerfile .
    docker tag <service>:latest 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    docker push 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    aws lambda update-function-code --function-name <service> --image-uri 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    ```

    Here's a shortcut where you specify the service name only once and you chain the commands (except the lambda):
    ```
    export service=<service> && docker build -t <service>:latest -f ./Dockerfiles/<service>.Dockerfile .
    docker tag <service>:latest 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    docker push 517478598677.dkr.ecr.us-east-2.amazonaws.com/<service>_service:latest
    ```

    This is all automated in `scripts/deploy_image_to_ecr.sh`, which will build
    a single service in Docker and deploy it to ECR. For example:
    ```
    ./scripts/deploy_single_image.sh feed_preprocessing
    ```
    Here is an example output (truncated):
    ```
    5f70bf18a086: Pushed 
    a7117f5b9430: Pushed 
    25aa4faae632: Pushed 
    205a1d2551b0: Pushed 
    bc8ce7b1ea70: Pushed 
    9365faa53226: Pushed 
    653e08820193: Pushed 
    58116b37f200: Pushed 
    4cc8dd7156f5: Pushed 
    611a3600cf7a: Pushed 
    02eecd1b8ebb: Pushed 
    latest: digest: sha256:3b636bebe5c40f9aa593b5ff43c15bca213b0e60d096621dd64e32d2fca2df8e size: 2619
    Deployment of feed_preprocessing completed successfully.
    ```
    Here is what a successfully deployed image should look like:
    ![Example ECR updated image](/assets/example_uploaded_ecr_image.png)


To run and validate an image locally:
```
docker build -f Dockerfiles/<service>.Dockerfile -t <service> .
docker run --name <service> <service>
docker exec -it <container-name> bash
docker logs <container-name>
docker stop <container-name>
docker rm <container-name>
docker rmi <image>
```