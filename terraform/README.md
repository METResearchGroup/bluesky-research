# Terraform

We use [Terraform](https://www.terraform.io/), a way to automate and programatically record our cloud infrastructure. We use this so that we can systematically make any changes to our cloud infrastructure and reproduce it as needed. For example, if we want to create a new lambda, instead of doing it via the AWS console, we define it in Terraform and then deploy our updated infrastructure configuration. This is also known as "Infrastructure-as-Code" (IaC).

We use a hybrid on-prem + AWS architecture, using AWS for some functionalities (e.g., making liberal use of Athena to analyze large-scale data) while using on-prem servers for others (e.g., running our DAGs).
