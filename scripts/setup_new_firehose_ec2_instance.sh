#!/bin/bash

# instructions for setting up a new ec2 instance for running the firehose
# pipeline.

# assumes that you have the ec2 instance already spun up.

# 1. Set up login creds for SSH
chmod 400 firehoseSyncEc2Key.pem

# 2. SSH into the instance
# replace with the correct instance address
ssh -i "firehoseSyncEc2Key.pem" ec2-user@ec2-18-188-39-255.us-east-2.compute.amazonaws.com

# 3. Basic setup
sudo yum update -y
sudo yum install git -y
sudo yum -y install docker

sudo service docker start
sudo usermod -a -G docker ec2-user

# exit instance, then SSH back in
exit
ssh -i "firehoseSyncEc2Key.pem" ec2-user@ec2-18-188-39-255.us-east-2.compute.amazonaws.com

# install Github CLI: https://github.com/cli/cli/blob/trunk/docs/install_linux.md#amazon-linux-2-yum
type -p yum-config-manager >/dev/null || sudo yum install yum-utils
sudo yum-config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
sudo yum install gh

# clone repo
git clone https://github.com/METResearchGroup/bluesky-research.git

# set up AWS access
aws configure # use the same creds as in .aws/config
aws sso configure # use same sso creds as in .aws/config
export AWS_PROFILE="AWSAdministratorAccess-517478598677"
ACCOUNT_ID=$(aws sts get-caller-identity --profile $AWS_PROFILE --query "Account" --output text)

# connect to ECR
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com

# get sync firehose image from ECR
export FIREHOSE_REPO="sync_firehose_stream_service"
docker pull ${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/$FIREHOSE_REPO:latest

# set up Cloudwatch logs for Docker container

# run container
export CONTAINER_NAME="sync-firehose-container"
docker run -d --name $CONTAINER_NAME --platform linux/arm64 ${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/$FIREHOSE_REPO:latest

# to stop a container
# docker stop $CONTAINER_NAME
# docker rm $CONTAINER_NAME
