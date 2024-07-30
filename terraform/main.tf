provider "aws" {
  region = var.aws_region
  profile = var.aws_profile

}

### ECR repos ###
resource "aws_ecr_repository" "add_users_to_study_service" {
  name = "add_users_to_study_service"
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
  image_uri = "${aws_ecr_repository.feed_api_service.repository_url}:latest"
  architectures = ["arm64"] # since images are built locally with an M1 Mac.

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

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

### IAM Roles and Policies ###

# Create IAM role for Lambda
# https://spacelift.io/blog/terraform-aws-lambda
resource "aws_iam_role" "lambda_exec" {
  name = "LambdaS3AccessRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}


resource "aws_iam_policy" "lambda_s3_policy" {
  name        = var.bsky_api_policy_name
  description = "IAM policy for Bluesky API Lambda to access S3."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
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
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

# S3 bucket policy, to allow read access for lambda.
resource "aws_s3_bucket_policy" "bluesky_research_bucket_policy" {
  bucket = var.s3_root_bucket_name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${aws_iam_role.lambda_exec.name}"
        },
        Action = [
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
