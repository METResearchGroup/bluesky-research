provider "aws" {
  region = var.aws_region
}

# ECR repos
resource "aws_ecr_repository" "feed_api_service" {
  name = "feed_api_service"
}

# Lambdas
resource "aws_lambda_function" "feed_api_lambda" {
  function_name = "feed_api_lambda"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.10"

  image_uri = "${aws_ecr_repository.feed_api_service.repository_url}:latest"

  lifecycle {
    ignore_changes = [image_uri]
  }
}

# Create IAM role for Lambda
# https://spacelift.io/blog/terraform-aws-lambda
resource "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"
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

# Attach policy to IAM role
resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
