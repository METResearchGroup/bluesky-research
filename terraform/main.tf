# certificate requested in us-east-1
# https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/cnames-and-https-requirements.html
provider "aws" {
  alias   = "us-east-1"
  region  = "us-east-1"
}

# most infra was build on us-east-2
provider "aws" {
  alias   = "us-east-2"
  region  = "us-east-2"
  profile = var.aws_profile
}


### ECR repos ###
resource "aws_ecr_repository" "add_users_to_study_service" {
  name = "add_users_to_study_service"
}

resource "aws_ecr_repository" "analytics_platform_service" {
  name = "analytics_platform_service"
}

resource "aws_ecr_repository" "calculate_superposters_service" {
  name = "calculate_superposters_service"
}

resource "aws_ecr_repository" "feed_api_service" {
  name = "feed_api_service"
}

resource "aws_ecr_repository" "ml_inference_perspective_api_service" {
  name = "ml_inference_perspective_api_service"
}

resource "aws_ecr_repository" "preprocess_raw_data_service" {
  name = "preprocess_raw_data_service"
}

resource "aws_ecr_repository" "sync_firehose_stream_service" {
  name = "sync_firehose_stream_service"
}

resource "aws_ecr_repository" "sync_most_liked_feed_service" {
  name = "sync_most_liked_feed_service"
}

### Lambdas ###
resource "aws_lambda_function" "bluesky_feed_api_lambda" {
  function_name = var.bsky_api_lambda_name
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.feed_api_service.repository_url}:latest"
  architectures = ["arm64"] # since images are built locally with an M1 Mac.
  timeout       = 15 # 15 second timeout.
  memory_size   = 256

  lifecycle {
    ignore_changes = [image_uri]
  }
}

### API Gateway ###

# define API Gateway REST API
resource "aws_api_gateway_rest_api" "bluesky_feed_api_gateway" {
  name        = "bluesky_feed_api_gateway"
  description = "Bluesky Feed API"
}

# Define root resource for /
resource "aws_api_gateway_method" "bluesky_feed_api_root_method" {
  rest_api_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  resource_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.root_resource_id
  http_method   = "GET"
  authorization = "NONE"
}

# Integrate GET method for / with lambda, using AWS proxy to forward requests
resource "aws_api_gateway_integration" "bluesky_feed_api_root_integration" {
  rest_api_id             = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  resource_id             = aws_api_gateway_rest_api.bluesky_feed_api_gateway.root_resource_id
  http_method             = aws_api_gateway_method.bluesky_feed_api_root_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.bluesky_feed_api_lambda.invoke_arn
}

# Define greedy path resource under /
resource "aws_api_gateway_resource" "bluesky_feed_api_proxy" {
  rest_api_id = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  parent_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.root_resource_id
  path_part   = "{proxy+}"
}

# Define ANY method for /{proxy+}
resource "aws_api_gateway_method" "bluesky_feed_api_proxy_method" {
  rest_api_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  resource_id   = aws_api_gateway_resource.bluesky_feed_api_proxy.id
  http_method   = "ANY"
  authorization = "NONE"
}

# Integrate ANY method with lambda, using AWS proxy to forward requests
resource "aws_api_gateway_integration" "bluesky_feed_api_proxy_integration" {
  rest_api_id             = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  resource_id             = aws_api_gateway_resource.bluesky_feed_api_proxy.id
  http_method             = aws_api_gateway_method.bluesky_feed_api_proxy_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.bluesky_feed_api_lambda.invoke_arn
  timeout_milliseconds    = 15000  # Set to 15 seconds, to match lambda.
}

# Deploy API
resource "aws_api_gateway_deployment" "bluesky_feed_api_gateway_deployment" {
  depends_on = [
    aws_api_gateway_integration.bluesky_feed_api_proxy_integration,
    aws_api_gateway_integration.bluesky_feed_api_root_integration
  ]

  rest_api_id = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  stage_name  = "prod"
}

# Lambda permission to allow API Gateway to invoke it
resource "aws_lambda_permission" "api_gateway_permission" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bluesky_feed_api_lambda.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.bluesky_feed_api_gateway.execution_arn}/*/*"
}

### Custom domain + API Gateway mapping ###
resource "aws_api_gateway_domain_name" "custom_domain" {
  domain_name = var.custom_domain_name
  regional_certificate_arn = var.acm_certificate_arn

  # we don't need edge-optimized. We don't need CloudFront CDNs to optimize
  # for global edge locations. We only need regional.
  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_base_path_mapping" "custom_domain_mapping" {
  api_id = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  stage_name = aws_api_gateway_stage.api_gateway_stage.stage_name
  domain_name = aws_api_gateway_domain_name.custom_domain.domain_name
}

### Add Route53 record for custom domain ###
data "aws_route53_zone" "selected" {
  name         = "mindtechnologylab.com."
  private_zone = false
}

# Route 53 Alias record to point to the API Gateway domain name
resource "aws_route53_record" "api_gateway_alias" {
  zone_id = data.aws_route53_zone.selected.zone_id
  name    = var.custom_domain_name
  type    = "A"

  alias {
    name                   = aws_api_gateway_domain_name.custom_domain.regional_domain_name
    zone_id                = aws_api_gateway_domain_name.custom_domain.regional_zone_id
    evaluate_target_health = false
  }
}


### Cloudwatch logging ###
resource "aws_iam_role" "api_gateway_cloudwatch_role" {
  name = "APIGatewayCloudWatchLogsRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "apigateway.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "api_gateway_cloudwatch_role_policy" {
  role       = aws_iam_role.api_gateway_cloudwatch_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
}

resource "aws_api_gateway_account" "api_gateway_account" {
  cloudwatch_role_arn = aws_iam_role.api_gateway_cloudwatch_role.arn
  depends_on = [aws_iam_role_policy_attachment.api_gateway_cloudwatch_role_policy]
}
resource "aws_cloudwatch_log_group" "api_gateway_log_group" {
  name              = "/aws/api-gateway/bluesky_feed_api_gateway"
  retention_in_days = 7
}

resource "aws_api_gateway_stage" "api_gateway_stage" {
  stage_name    = "prod"
  rest_api_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  deployment_id = aws_api_gateway_deployment.bluesky_feed_api_gateway_deployment.id  # Corrected reference

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway_log_group.arn
    format          = jsonencode({
      requestId       = "$context.requestId"
      ip              = "$context.identity.sourceIp"
      caller          = "$context.identity.caller"
      user            = "$context.identity.user"
      requestTime     = "$context.requestTime"
      httpMethod      = "$context.httpMethod"
      resourcePath    = "$context.resourcePath"
      status          = "$context.status"
      protocol        = "$context.protocol"
      responseLength  = "$context.responseLength"
    })
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_api_gateway_account.api_gateway_account]
}

### IAM Roles and Policies ###

# Create IAM role for Lambda
# https://spacelift.io/blog/terraform-aws-lambda
resource "aws_iam_role" "lambda_exec" {
  name = "LambdaAccessRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}

# Define new IAM policy for Secrets Manager and S3
resource "aws_iam_policy" "lambda_access_policy" {
  name        = "LambdaSecretsAndS3Policy"
  description = "IAM policy for Bluesky API Lambda to access S3 and Secrets Manager."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # Add S3 policy
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetObjectVersion",
          "s3:GetObjectAcl",
          "s3:GetObjectTagging"
        ],
        Effect   = "Allow",
        Resource = [
          "arn:aws:s3:::${var.s3_root_bucket_name}",
          "arn:aws:s3:::${var.s3_root_bucket_name}/*"
        ]
      },
      # Add Secrets Manager policy
      {
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Effect   = "Allow",
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:bluesky_account_credentials-cX3wOk",
          "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:bsky-internal-api-key-jNloNG"
        ]
      },
      # Add DynamoDB policy
      {
        Action = [
          "dynamodb:PutItem"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/study_participants"
      },
      # Add CloudWatch Logs policy
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.bsky_api_lambda_name}:*"
      }
    ]
  })
}

# Attach the new policy to the Lambda execution role
resource "aws_iam_role_policy_attachment" "lambda_attach_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_access_policy.arn
}

# Attach existing policy for CloudWatch logs to the Lambda execution role
resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_access_policy.arn
}

# S3 bucket policy, to allow read access for lambda.
resource "aws_s3_bucket_policy" "bluesky_research_bucket_policy" {
  provider = aws.us-east-2  # Specify the correct provider for us-east-2
  bucket   = var.s3_root_bucket_name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${aws_iam_role.lambda_exec.name}"
        },
        Action   = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetObjectVersion",
          "s3:GetObjectAcl",
          "s3:GetObjectTagging"
        ],
        Resource = [
          "arn:aws:s3:::${var.s3_root_bucket_name}",
          "arn:aws:s3:::${var.s3_root_bucket_name}/*"
        ]
      }
    ]
  })
}

data "aws_caller_identity" "current" {}

### Elastic Beanstalk ###
resource "aws_elastic_beanstalk_application" "analytics_platform" {
  name        = "analytics-platform"
  description = "Analytics Platform Streamlit App"
}

# Create a VPC
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "main-vpc"
  }
}

# Create an Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "main-igw"
  }
}

# Create a public subnet
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "public-subnet-${count.index + 1}"
  }
}

# Create a private subnet
resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 101}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "private-subnet-${count.index + 1}"
  }
}

# Create a route table for public subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "public-route-table"
  }
}

# Associate public subnets with the public route table
resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Get available AZs
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_elastic_beanstalk_environment" "analytics_platform_env" {
  name                = "analytics-platform-env"
  application         = aws_elastic_beanstalk_application.analytics_platform.name
  solution_stack_name = "64bit Amazon Linux 2023 v4.3.5 running Docker"

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "IamInstanceProfile"
    value     = aws_iam_instance_profile.eb_instance_profile.name
  }

  setting {
    namespace = "aws:elasticbeanstalk:application:environment"
    name      = "DOCKER_REGISTRY_URL"
    value     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
  }

  setting {
    namespace = "aws:elasticbeanstalk:application:environment"
    name      = "DOCKER_IMAGE"
    value     = "${aws_ecr_repository.analytics_platform_service.repository_url}:latest"
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "EnvironmentType"
    value     = "SingleInstance"
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "LoadBalancerType"
    value     = "application"
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment:process:default"
    name      = "Port"
    value     = "8501"  # The port your Streamlit app is running on
  }

  # Add VPC configuration
  setting {
    namespace = "aws:ec2:vpc"
    name      = "VPCId"
    value     = aws_vpc.main.id
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "Subnets"
    value     = join(",", aws_subnet.private[*].id)
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "ELBSubnets"
    value     = join(",", aws_subnet.public[*].id)
  }
}

# IAM role for Elastic Beanstalk EC2 instances
resource "aws_iam_role" "eb_ec2_role" {
  name = "eb-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# Attach policies to the Elastic Beanstalk EC2 role
resource "aws_iam_role_policy_attachment" "eb_web_tier" {
  policy_arn = "arn:aws:iam::aws:policy/AWSElasticBeanstalkWebTier"
  role       = aws_iam_role.eb_ec2_role.name
}

resource "aws_iam_role_policy_attachment" "eb_worker_tier" {
  policy_arn = "arn:aws:iam::aws:policy/AWSElasticBeanstalkWorkerTier"
  role       = aws_iam_role.eb_ec2_role.name
}

resource "aws_iam_role_policy_attachment" "eb_ecr_read_only" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eb_ec2_role.name
}

# Create an instance profile for Elastic Beanstalk
resource "aws_iam_instance_profile" "eb_instance_profile" {
  name = "eb-instance-profile"
  role = aws_iam_role.eb_ec2_role.name
}

# Output the URL of the deployed application
output "analytics_platform_url" {
  value = aws_elastic_beanstalk_environment.analytics_platform_env.cname
}