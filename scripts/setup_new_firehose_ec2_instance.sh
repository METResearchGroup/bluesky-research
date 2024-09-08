#!/bin/bash

# instructions for setting up a new ec2 instance for running the firehose
# pipeline.

# assumes that you have the ec2 instance already spun up.

# (step 0: create EC2 instance)
aws ec2 run-instances \
    --image-id ami-067df2907035c28c2 \
    --count 1 \
    --instance-type t4g.small \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=firehose-sync-ec2-instance-arm64}]' \
    --iam-instance-profile Name=EC2InstanceProfile \
    --key-name firehoseSyncEc2Key \
	--security-group-ids sg-0b3e24638f16807d5 # "launch-wizard

# 1. Set up login creds for SSH
chmod 400 firehoseSyncEc2Key.pem

# 2. SSH into the instance
# replace with the correct instance address
ssh -i "firehoseSyncEc2Key.pem" ec2-user@ec2-3-129-70-136.us-east-2.compute.amazonaws.com

# 3. Basic setup
sudo yum update -y
sudo yum install git -y
# sudo yum -y install docker
# sudo service docker start

# sudo service docker start
# sudo usermod -a -G docker ec2-user

# Configure Docker log rotation. This is so that we don't accumulate a ton of
# logs in the EC2 instance. We're not using a large instance, so we do need to
# limit the amount of space we're using. The logs are stored in Cloudwatch, so
# we don't need to worry about filling up the disk.
# sudo tee /etc/docker/daemon.json > /dev/null <<EOL
# {
#   "log-driver": "json-file",
#   "log-opts": {
#     "max-size": "10m",
#     "max-file": "3"
#   }
# }
# EOL

# Restart Docker to apply the new configuration
# sudo systemctl restart docker

# exit instance, then SSH back in
exit
ssh -i "firehoseSyncEc2Key.pem" ec2-user@ec2-3-129-70-136.us-east-2.compute.amazonaws.com

# install Github CLI: https://github.com/cli/cli/blob/trunk/docs/install_linux.md#amazon-linux-2-yum
type -p yum-config-manager >/dev/null || sudo yum install yum-utils
sudo yum-config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
sudo yum install gh

# login to Github
gh auth login

# clone repo
git clone https://github.com/METResearchGroup/bluesky-research.git

# add pythonpath (ONLY if you're setting up a new instance)
echo "export PYTHONPATH=/home/ec2-user/bluesky-research:$PYTHONPATH" >> ~/.bashrc && source ~/.bashrc

# add default region (ONLY if you're setting up a new instance)
echo 'export AWS_REGION="us-east-2"' >> ~/.bashrc && source ~/.bashrc

# set up AWS access
# aws configure # use the same creds as in .aws/config
# aws configure sso # use the same creds as in .aws/config
# export AWS_PROFILE="AWSAdministratorAccess-517478598677"
# ACCOUNT_ID=$(aws sts get-caller-identity --profile ${AWS_PROFILE} --query "Account" --output text)

# connect to ECR
# aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com

# get sync firehose image from ECR
# export FIREHOSE_REPO="sync_firehose_stream_service"
# docker pull ${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/${FIREHOSE_REPO}:latest

# run container (with the correct AWS credentials mounted)
# export CONTAINER_NAME="sync-firehose-container"
# export CONTAINER_ID=$(docker run -d --name ${CONTAINER_NAME} --platform linux/arm64 \
#      ${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/${FIREHOSE_REPO}:latest)

# set up Cloudwatch logs for Docker container
# aws logs create-log-group --log-group-name sync-firehose-logs # should already exist.

# NOTE: if rerunning an existence instance, just need to run the CloudWatch
# agent steps and then run nohup. Do NOT try to auth to AWS or do anything
# that interacts with the AWS cli. This will cause the job to look for the
# AWS profile that will be in .aws/config. The EC2 instance does not have
# this profile; instead, it has a profile that is attached to the instance.

# Install the CloudWatch agent (ONLY if you're setting up a new instance)
sudo yum install -y amazon-cloudwatch-agent

# Create the CloudWatch agent configuration file
# NOTE: can comment out the Docker log files if we don't run in a container.
sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json > /dev/null <<EOL
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/home/ec2-user/bluesky-research/lib/log/*.log",
            "log_group_name": "sync-firehose-logs",
            "log_stream_name": "{instance_id}/bsky-logs"
          },
          {
            "file_path": "/home/ec2-user/bluesky-research/pipelines/sync_post_records/firehose/nohup.out",
            "log_group_name": "sync-firehose-logs",
            "log_stream_name": "nohup-logs-{instance_id}"
          }
        ]
      }
    }
  }
}
EOL

# Start the CloudWatch agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a start
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s

# run the firehose
cd /home/ec2-user/bluesky-research/pipelines/sync_post_records/firehose
nohup python firehose.py > output.log 2>&1 & tail -f output.log

# Optional: stop and restart the CloudWatch agent
# if you need to change the config or if you are restarting the Docker container
# sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a stop -m ec2
# sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s
# To check logs for Cloudwatch agent:
# sudo tail -f /opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log
# To get status for Cloudwatch agent:
# sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status
# to stop a container
# docker stop $CONTAINER_NAME
# docker rm $CONTAINER_NAME

# list all containers
# docker ps
# docker ps -a # including stopped containers

# see logs for a container
# docker logs $CONTAINER_NAME

# inspect a container
# docker inspect --format='{{.State.Status}}' sync-firehose-container
# docker inspect sync-firehose-container
