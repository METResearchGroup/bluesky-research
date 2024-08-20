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

# add 1-day TTL to the daily superposter data
resource "aws_s3_bucket_lifecycle_configuration" "daily_superposter_posts_lifecycle" {
  bucket = "bluesky-research"

  rule {
    id     = "DeleteDailyPostsAfterOneDay"
    status = "Enabled"

    filter {
      prefix = "daily-posts/"
    }

    expiration {
      days = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results_lifecycle" {
  bucket = "bluesky-research"

  rule {
    id     = "DeleteAthenaResultsAfterOneDay"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 1
    }
  }
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
  image_uri     = "${aws_ecr_repository.feed_api_service.repository_url}:latest"
  architectures = ["arm64"] # since images are built locally with an M1 Mac.
  timeout       = 15 # 15 second timeout.
  memory_size   = 256

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_lambda_function" "preprocess_raw_data_lambda" {
  function_name = "preprocess_raw_data_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.preprocess_raw_data_service.repository_url}:latest"
  architectures = ["arm64"] # since images are built locally with an M1 Mac.
  timeout       = 180 # 3 minute timeout
  memory_size   = 1024 # 1 GB of memory

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "preprocess_raw_data_lambda_log_group" {
  name              = "/aws/lambda/preprocess_raw_data_lambda"
  retention_in_days = 7
}

resource "aws_lambda_function" "calculate_superposters_lambda" {
  function_name = "calculate_superposters_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.calculate_superposters_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 90 # 90 seconds timeout
  memory_size   = 512 # 512 MB of memory

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "calculate_superposters_lambda_log_group" {
  name              = "/aws/lambda/calculate_superposters_lambda"
  retention_in_days = 7
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
          "s3:AbortMultipartUpload",
          "s3:CreateBucket",
          "s3:GetObject",
          "s3:GetBucketLocation", # https://repost.aws/knowledge-center/athena-output-bucket-error
          "s3:GetObjectVersion",
          "s3:GetObjectAcl",
          "s3:GetObjectTagging",
          "s3:ListBucket",
          "s3:ListBuckets",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:ListObjects",
          "s3:PutObject",
          "s3:PutObjectAcl"
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
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/*"
      },
      # Add CloudWatch Logs policy
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"
      },
      # Add SQS policy
      {
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      # Add Glue policy for accessing Glue tables
      {
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions"
        ],
        Effect   = "Allow",
        Resource = "*"
      },
      # Add Athena policy for accessing Athena tables
      {
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Effect   = "Allow",
        Resource = "*"
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

# add IAM policy for CloudWatch agent for EC2 instances
resource "aws_iam_role" "cloudwatch_ec2_instance_agent_role" {
  name = "CloudWatchAgentServerRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "cloudwatch_agent_policy" {
  name        = "CloudWatchAgentPolicy"
  description = "IAM policy for CloudWatch Agent to access CloudWatch Logs."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent_attach_policy" {
  role       = aws_iam_role.cloudwatch_ec2_instance_agent_role.name
  policy_arn = aws_iam_policy.cloudwatch_agent_policy.arn
}

resource "aws_iam_instance_profile" "cloudwatch_agent_instance_profile" {
  name = "CloudWatchAgentInstanceProfile"
  role = aws_iam_role.cloudwatch_ec2_instance_agent_role.name
}
### SQS Queue ###
resource "aws_sqs_queue" "syncs_to_be_processed_queue" {
  name                      = "syncsToBeProcessedQueue.fifo"
  fifo_queue                = true
  content_based_deduplication = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter_queue.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "dead_letter_queue" {
  name = "syncsToBeProcessedDLQ.fifo"
  fifo_queue = true
}

resource "aws_sqs_queue" "firehose_syncs_to_be_processed_queue" {
  name                      = "firehoseSyncsToBeProcessedQueue.fifo"
  fifo_queue                = true
  content_based_deduplication = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.firehose_dead_letter_queue.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "firehose_dead_letter_queue" {
  name = "firehoseSyncsToBeProcessedDLQ.fifo"
  fifo_queue = true
}

resource "aws_sqs_queue" "most_liked_syncs_to_be_processed_queue" {
  name                      = "mostLikedSyncsToBeProcessedQueue.fifo"
  fifo_queue                = true
  content_based_deduplication = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.most_liked_dead_letter_queue.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "most_liked_dead_letter_queue" {
  name = "mostLikedSyncsToBeProcessedDLQ.fifo"
  fifo_queue = true
}


### IAM Policies for SQS ###
resource "aws_iam_role_policy" "lambda_sqs_policy" {
  name   = "LambdaSQSPolicy"
  role   = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ],
        Effect   = "Allow",
        Resource = aws_sqs_queue.syncs_to_be_processed_queue.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach_sqs_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_access_policy.arn
}

### AWS Glue ###

# Glue DB for the daily superposter data
resource "aws_glue_catalog_database" "default" {
  name = "default_db"
}

resource "aws_glue_catalog_table" "daily_posts" {
  database_name = aws_glue_catalog_database.default.name
  name          = "daily_posts"

  storage_descriptor {
    columns {
      name = "author_did" # DID of the post author.
      type = "string"
    }
    columns {
      name = "uri" # URI of the post.
      type = "string"
    }

    location      = "s3://${var.s3_root_bucket_name}/daily-posts/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }

  table_type = "EXTERNAL_TABLE"
}

### AWS Athena ###

# set default workgroup and output location for said workgroup.
resource "aws_athena_workgroup" "prod_workgroup" {
  name = "prod_workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${var.s3_root_bucket_name}/athena-results/"
    }
  }
}

### DynamoDB ###
resource "aws_dynamodb_table" "superposters" {
  name           = "superposters"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "insert_date_timestamp"

  attribute {
    name = "insert_date_timestamp"
    type = "S"
  }

  tags = {
    Name = "superposters"
  }
}
