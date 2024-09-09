#!/bin/bash

# Instructions for setting up the Feed API on an EC2 instance.

# 1. Set up login creds for SSH
chmod 400 firehoseSyncEc2Key.pem

# 2. SSH into the instance
# replace with the correct instance address
ssh -i "firehoseSyncEc2Key.pem" ec2-user@ec2-3-135-20-246.us-east-2.compute.amazonaws.com

# 3. Basic setup
sudo yum update -y
sudo yum install git -y
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

# set up venv with Miniconda
# https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
# then restart the terminal
# after restarting, do the following:
# type "conda", to verify that it works.
conda create --name bluesky-research python=3.10 -y
conda activate bluesky-research
cd /home/ec2-user/bluesky-research/feed_api
pip install -r requirements.txt

# Install the CloudWatch agent (ONLY if you're setting up a new instance)
sudo yum install -y amazon-cloudwatch-agent

# create directory in case it doesn't exist.
sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc/

# Create the CloudWatch agent configuration file
sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json > /dev/null <<EOL
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/home/ec2-user/bluesky-research/feed_api/nohup.out",
            "log_group_name": "ec2-feed-api-logs",
            "log_stream_name": "{instance_id}/bsky-logs"
          }
        ]
      }
    }
  }
}
EOL

# verify file exists
# ls -l /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# Start the CloudWatch agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a start
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s

# to kill the process, find the ID and then kill -9 <id>
# sudo lsof -i :8000
# to run the app with nohup
# nohup python app.py &