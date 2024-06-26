# Terraform

We use [Terraform](https://www.terraform.io/), a way to automate and programatically record our cloud infrastructure. We use this so that we can systematically make any changes to our cloud infrastructure and reproduce it as needed. For example, if we want to create a new lambda, instead of doing it via the AWS console, we define it in Terraform and then deploy our updated infrastructure configuration. This is also known as "Infrastructure-as-Code" (IaC).

We can install Terraform using [these instructions](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli). [Here](https://developer.hashicorp.com/terraform/tutorials/aws-get-started) is a good set of instructions introducing not only Terraform, but how to integrate Terraform with AWS.

Steps to set up (defined in [this](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/aws-build) guide):
- Install Terraform
- Set up AWS cli (if needed) and define your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` variables (Terraform will look for these in order to have access to AWS resources)
- Initialize your Terraform repository. Go to `./terraform` and then run `terraform init`.
- Run `terraform fmt` and `terraform validate` to update the format of your configuration and to validate the files.
- Run `terraform plan` to see the plan of what Terraform will change in the infrastructure. It looks similar to Git.
```plaintext
(bluesky-research) ➜  terraform git:(main) ✗ terraform plan    

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # aws_ecr_repository.feed_api_service will be created
  + resource "aws_ecr_repository" "feed_api_service" {
      + arn                  = (known after apply)
      + id                   = (known after apply)
      + image_tag_mutability = "MUTABLE"
      + name                 = "feed_api_service"
      + registry_id          = (known after apply)
      + repository_url       = (known after apply)
      + tags_all             = (known after apply)
    }

  # aws_iam_role.lambda_exec will be created
  + resource "aws_iam_role" "lambda_exec" {
      + arn                   = (known after apply)
      + assume_role_policy    = jsonencode(
            {
              + Statement = [
                  + {
                      + Action    = "sts:AssumeRole"
                      + Effect    = "Allow"
                      + Principal = {
                          + Service = "lambda.amazonaws.com"
                        }
                    },
                ]
              + Version   = "2012-10-17"
            }
        )
      + create_date           = (known after apply)
      + force_detach_policies = false
      + id                    = (known after apply)
      + managed_policy_arns   = (known after apply)
      + max_session_duration  = 3600
      + name                  = "lambda_exec_role"
      + name_prefix           = (known after apply)
      + path                  = "/"
      + tags_all              = (known after apply)
      + unique_id             = (known after apply)

      + inline_policy (known after apply)
    }

  # aws_iam_role_policy_attachment.lambda_policy will be created
  + resource "aws_iam_role_policy_attachment" "lambda_policy" {
      + id         = (known after apply)
      + policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
      + role       = "lambda_exec_role"
    }

  # aws_lambda_function.feed_api_lambda will be created
  + resource "aws_lambda_function" "feed_api_lambda" {
      + architectures                  = (known after apply)
      + arn                            = (known after apply)
      + code_sha256                    = (known after apply)
      + function_name                  = "feed_api_lambda"
      + handler                        = "lambda_function.lambda_handler"
      + id                             = (known after apply)
      + image_uri                      = (known after apply)
      + invoke_arn                     = (known after apply)
      + last_modified                  = (known after apply)
      + memory_size                    = 128
      + package_type                   = "Zip"
      + publish                        = false
      + qualified_arn                  = (known after apply)
      + qualified_invoke_arn           = (known after apply)
      + reserved_concurrent_executions = -1
      + role                           = (known after apply)
      + runtime                        = "python3.10"
      + signing_job_arn                = (known after apply)
      + signing_profile_version_arn    = (known after apply)
      + skip_destroy                   = false
      + source_code_hash               = (known after apply)
      + source_code_size               = (known after apply)
      + tags_all                       = (known after apply)
      + timeout                        = 3
      + version                        = (known after apply)

      + ephemeral_storage (known after apply)

      + logging_config (known after apply)

      + tracing_config (known after apply)
    }

Plan: 4 to add, 0 to change, 0 to destroy.
```
- Once you're happy with the result, run `terraform apply`. Note that changes can be destructive as well, so be careful with any changes that cause a removal of a resource (Terraform will warn you of this in the logs but be wary of it as well).
- Terraform will ask you to confirm:
```plaintext
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes
```
- Once that is confirmed, check the AWS console (might take a minute or two) to see that the intended changes have been made.