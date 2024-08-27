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
  memory_size   = 512

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

# Create IAM role for EC2.
resource "aws_iam_role" "ec2_instance_role" {
  name = "EC2InstanceAccessRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "EC2InstanceProfile"
  role = aws_iam_role.ec2_instance_role.name
}

# Give the EC2 role the same policy as the lambda (so it can do the
# same things).
resource "aws_iam_role_policy_attachment" "ec2_instance_attach_policy" {
  role       = aws_iam_role.ec2_instance_role.name
  policy_arn = aws_iam_policy.lambda_access_policy.arn
}

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
          "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:bsky-internal-api-key-jNloNG",
          "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:momento_credentials-FhSxD6",
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
      },
      # Add Elasticache policy to be able to access Elasticache
      {
        Action = [
          "elasticache:*",
        ],
        Effect   = "Allow",
        Resource = "*"
      },
      # Glue crawler permissions
      {
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlerMetrics",
          "glue:GetCrawls",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions"
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
  name = var.default_glue_database_name
}

# Glue table for tracking daily posts (for superposter calculation)
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

# Glue table for user social networks
resource "aws_glue_catalog_table" "user_social_networks" {
  database_name = aws_glue_catalog_database.default.name
  name          = "user_social_networks"

  storage_descriptor {
    columns {
      name = "follow_handle"
      type = "string"
    }
    columns {
      name = "follow_url"
      type = "string"
    }
    columns {
      name = "follow_did"
      type = "string"
    }
    columns {
      name = "follower_handle"
      type = "string"
    }
    columns {
      name = "follower_url"
      type = "string"
    }
    columns {
      name = "follower_did"
      type = "string"
    }
    columns {
      name = "insert_timestamp"
      type = "string"
    }
    columns {
      name = "relationship_to_study_user"
      type = "string"
    }

    location      = "s3://${var.s3_root_bucket_name}/scraped-user-social-network/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }

  table_type = "EXTERNAL_TABLE"
}

# Glue table for preprocessed firehose posts
resource "aws_glue_catalog_table" "preprocessed_firehose_posts" {
  database_name = aws_glue_catalog_database.default.name
  name          = "preprocessed_firehose_posts"

  table_type = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/preprocessed_data/post/firehose/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "cid"
      type = "string"
    }
    columns {
      name = "indexed_at"
      type = "string"
    }
    columns {
      name = "author_did"
      type = "string"
    }
    columns {
      name = "author_handle"
      type = "string"
    }
    columns {
      name = "author_avatar"
      type = "string"
    }
    columns {
      name = "author_display_name"
      type = "string"
    }
    columns {
      name = "created_at"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "embed"
      type = "string"
    }
    columns {
      name = "entities"
      type = "string"
    }
    columns {
      name = "facets"
      type = "string"
    }
    columns {
      name = "labels"
      type = "string"
    }
    columns {
      name = "langs"
      type = "string"
    }
    columns {
      name = "reply_parent"
      type = "string"
    }
    columns {
      name = "reply_root"
      type = "string"
    }
    columns {
      name = "tags"
      type = "string"
    }
    columns {
      name = "synctimestamp"
      type = "string"
    }
    columns {
      name = "url"
      type = "string"
    }
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "like_count"
      type = "string"
    }
    columns {
      name = "reply_count"
      type = "string"
    }
    columns {
      name = "repost_count"
      type = "string"
    }
    columns {
      name = "passed_filters"
      type = "string"
    }
    columns {
      name = "filtered_at"
      type = "string"
    }
    columns {
      name = "filtered_by_func"
      type = "string"
    }
    columns {
      name = "preprocessing_timestamp"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
  partition_keys {
    name = "minute"
    type = "string"
  }
}

# Glue table for preprocessed most liked posts
resource "aws_glue_catalog_table" "preprocessed_most_liked_posts" {
  name          = "preprocessed_most_liked_posts"
  database_name = aws_glue_catalog_database.default.name

  table_type = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/preprocessed_data/post/most_liked/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "cid"
      type = "string"
    }
    columns {
      name = "indexed_at"
      type = "string"
    }
    columns {
      name = "author_did"
      type = "string"
    }
    columns {
      name = "author_handle"
      type = "string"
    }
    columns {
      name = "author_avatar"
      type = "string"
    }
    columns {
      name = "author_display_name"
      type = "string"
    }
    columns {
      name = "created_at"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "embed"
      type = "string"
    }
    columns {
      name = "entities"
      type = "string"
    }
    columns {
      name = "facets"
      type = "string"
    }
    columns {
      name = "labels"
      type = "string"
    }
    columns {
      name = "langs"
      type = "string"
    }
    columns {
      name = "reply_parent"
      type = "string"
    }
    columns {
      name = "reply_root"
      type = "string"
    }
    columns {
      name = "tags"
      type = "string"
    }
    columns {
      name = "synctimestamp"
      type = "string"
    }
    columns {
      name = "url"
      type = "string"
    }
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "like_count"
      type = "string"
    }
    columns {
      name = "reply_count"
      type = "string"
    }
    columns {
      name = "repost_count"
      type = "string"
    }
    columns {
      name = "passed_filters"
      type = "string"
    }
    columns {
      name = "filtered_at"
      type = "string"
    }
    columns {
      name = "filtered_by_func"
      type = "string"
    }
    columns {
      name = "preprocessing_timestamp"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
  partition_keys {
    name = "minute"
    type = "string"
  }
}


# Glue crawler, to make sure that new partitions are registered and added
# to the respective Glue tables.
resource "aws_iam_role" "glue_crawler_role" {
  name = "glue_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_policy" "glue_crawler_policy" {
  name        = "glue_crawler_policy"
  description = "Policy for Glue Crawler to access S3 and Glue Data Catalog"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::bluesky-research",
          "arn:aws:s3:::bluesky-research/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:BatchCreatePartition",
          "glue:BatchUpdatePartition",
          "glue:GetPartition",
          "glue:GetPartitions"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_crawler_attach_policy" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_crawler_policy.arn
}

resource "aws_glue_crawler" "preprocessed_posts_crawler" {
  name        = "preprocessed_posts_crawler"
  role        = aws_iam_role.glue_crawler_role.arn
  database_name = var.default_glue_database_name

  s3_target {
    path = "s3://${var.s3_root_bucket_name}/preprocessed_data/post/most_liked/"
  }

  s3_target {
    path = "s3://${var.s3_root_bucket_name}/preprocessed_data/post/firehose/"
  }

  schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

  configuration = jsonencode({
    "Version" = 1.0,
    "CrawlerOutput" = {
      "Partitions" = {
        "AddOrUpdateBehavior" = "InheritFromTable"
      }
    }
  })
}

resource "aws_glue_catalog_table" "perspective_api_firehose_labels" {
  name          = "perspective_api_firehose_labels"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://bluesky-research/ml_inference_perspective_api/firehose/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "perspective_api_firehose_labels_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "was_successfully_labeled"
      type = "boolean"
    }
    columns {
      name = "reason"
      type = "string"
    }
    columns {
      name = "label_timestamp"
      type = "string"
    }
    columns {
      name = "prob_toxic"
      type = "float"
    }
    columns {
      name = "prob_severe_toxic"
      type = "float"
    }
    columns {
      name = "prob_identity_attack"
      type = "float"
    }
    columns {
      name = "prob_insult"
      type = "float"
    }
    columns {
      name = "prob_profanity"
      type = "float"
    }
    columns {
      name = "prob_threat"
      type = "float"
    }
    columns {
      name = "prob_affinity"
      type = "float"
    }
    columns {
      name = "prob_compassion"
      type = "float"
    }
    columns {
      name = "prob_constructive"
      type = "float"
    }
    columns {
      name = "prob_curiosity"
      type = "float"
    }
    columns {
      name = "prob_nuance"
      type = "float"
    }
    columns {
      name = "prob_personal_story"
      type = "float"
    }
    columns {
      name = "prob_reasoning"
      type = "float"
    }
    columns {
      name = "prob_respect"
      type = "float"
    }
    columns {
      name = "prob_alienation"
      type = "float"
    }
    columns {
      name = "prob_fearmongering"
      type = "float"
    }
    columns {
      name = "prob_generalization"
      type = "float"
    }
    columns {
      name = "prob_moral_outrage"
      type = "float"
    }
    columns {
      name = "prob_scapegoating"
      type = "float"
    }
    columns {
      name = "prob_sexually_explicit"
      type = "float"
    }
    columns {
      name = "prob_flirtation"
      type = "float"
    }
    columns {
      name = "prob_spam"
      type = "float"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
  partition_keys {
    name = "minute"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "perspective_api_most_liked_labels" {
  name          = "perspective_api_most_liked_labels"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://bluesky-research/ml_inference_perspective_api/most_liked/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "perspective_api_most_liked_labels_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "was_successfully_labeled"
      type = "boolean"
    }
    columns {
      name = "reason"
      type = "string"
    }
    columns {
      name = "label_timestamp"
      type = "string"
    }
    columns {
      name = "prob_toxic"
      type = "float"
    }
    columns {
      name = "prob_severe_toxic"
      type = "float"
    }
    columns {
      name = "prob_identity_attack"
      type = "float"
    }
    columns {
      name = "prob_insult"
      type = "float"
    }
    columns {
      name = "prob_profanity"
      type = "float"
    }
    columns {
      name = "prob_threat"
      type = "float"
    }
    columns {
      name = "prob_affinity"
      type = "float"
    }
    columns {
      name = "prob_compassion"
      type = "float"
    }
    columns {
      name = "prob_constructive"
      type = "float"
    }
    columns {
      name = "prob_curiosity"
      type = "float"
    }
    columns {
      name = "prob_nuance"
      type = "float"
    }
    columns {
      name = "prob_personal_story"
      type = "float"
    }
    columns {
      name = "prob_reasoning"
      type = "float"
    }
    columns {
      name = "prob_respect"
      type = "float"
    }
    columns {
      name = "prob_alienation"
      type = "float"
    }
    columns {
      name = "prob_fearmongering"
      type = "float"
    }
    columns {
      name = "prob_generalization"
      type = "float"
    }
    columns {
      name = "prob_moral_outrage"
      type = "float"
    }
    columns {
      name = "prob_scapegoating"
      type = "float"
    }
    columns {
      name = "prob_sexually_explicit"
      type = "float"
    }
    columns {
      name = "prob_flirtation"
      type = "float"
    }
    columns {
      name = "prob_spam"
      type = "float"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
  partition_keys {
    name = "minute"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "llm_sociopolitical_firehose_labels" {
  name          = "llm_sociopolitical_firehose_labels"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/ml_inference_sociopolitical/firehose/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "llm_sociopolitical_firehose_labels_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "llm_model_name"
      type = "string"
    }
    columns {
      name = "was_successfully_labeled"
      type = "boolean"
    }
    columns {
      name = "reason"
      type = "string"
    }
    columns {
      name = "label_timestamp"
      type = "string"
    }
    columns {
      name = "is_sociopolitical"
      type = "boolean"
    }
    columns {
      name = "political_ideology_label"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
  partition_keys {
    name = "minute"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "llm_sociopolitical_most_liked_labels" {
  name          = "llm_sociopolitical_most_liked_labels"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/ml_inference_sociopolitical/most_liked/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "llm_sociopolitical_most_liked_labels_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "llm_model_name"
      type = "string"
    }
    columns {
      name = "was_successfully_labeled"
      type = "boolean"
    }
    columns {
      name = "reason"
      type = "string"
    }
    columns {
      name = "label_timestamp"
      type = "string"
    }
    columns {
      name = "is_sociopolitical"
      type = "boolean"
    }
    columns {
      name = "political_ideology_label"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
  partition_keys {
    name = "minute"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "in_network_embeddings" {
  name          = "in_network_embeddings"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/vector_embeddings/in_network_post_embeddings/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "uri"
      type = "string"
    }
    # columns { # Athena doesn't like the multi-nested field.
    #   name = "embedding"
    #   type = "array<array<array<double>>>"
    # }
    columns {
      name = "embedding_model"
      type = "string"
    }
    columns {
      name = "insert_timestamp"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "most_liked_feed_embeddings" {
  name          = "most_liked_feed_embeddings"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/vector_embeddings/most_liked_post_embeddings/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "uri"
      type = "string"
    }
    # columns { # Athena doesn't like the multi-nested field.
    #   name = "embedding"
    #   type = "array<array<array<double>>>"
    # }
    columns {
      name = "embedding_model"
      type = "string"
    }
    columns {
      name = "insert_timestamp"
      type = "string"
    }
  }
}


resource "aws_glue_catalog_table" "average_most_liked_feed_embeddings" {
  name          = "average_most_liked_feed_embeddings"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/vector_embeddings/average_most_liked_feed_embeddings/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "uris"
      type = "array<string>"
    }
    # columns { # Athena doesn't like the multi-nested field.
    #   name = "embedding"
    #   type = "array<array<array<double>>>"
    # }
    columns {
      name = "embedding_model"
      type = "string"
    }
    columns {
      name = "insert_timestamp"
      type = "string"
    }
  }
}
resource "aws_glue_catalog_table" "post_cosine_similarity_scores" {
  name          = "post_cosine_similarity_scores"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/vector_embeddings/similarity_scores/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "similarity_score"
      type = "double"
    }
    columns {
      name = "most_liked_average_embedding_key"
      type = "string"
    }
    columns {
      name = "insert_timestamp"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "consolidated_enriched_post_records" {
  name          = "consolidated_enriched_post_records"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "classification"      = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/consolidated_enriched_post_records/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "uri"
      type = "string"
    }
    columns {
      name = "cid"
      type = "string"
    }
    columns {
      name = "indexed_at"
      type = "string"
    }
    columns {
      name = "author_did"
      type = "string"
    }
    columns {
      name = "author_handle"
      type = "string"
    }
    columns {
      name = "author_avatar"
      type = "string"
    }
    columns {
      name = "author_display_name"
      type = "string"
    }
    columns {
      name = "created_at"
      type = "string"
    }
    columns {
      name = "text"
      type = "string"
    }
    columns {
      name = "embed"
      type = "string"
    }
    columns {
      name = "entities"
      type = "string"
    }
    columns {
      name = "facets"
      type = "string"
    }
    columns {
      name = "labels"
      type = "string"
    }
    columns {
      name = "langs"
      type = "string"
    }
    columns {
      name = "reply_parent"
      type = "string"
    }
    columns {
      name = "reply_root"
      type = "string"
    }
    columns {
      name = "tags"
      type = "string"
    }
    columns {
      name = "synctimestamp"
      type = "string"
    }
    columns {
      name = "url"
      type = "string"
    }
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "like_count"
      type = "int"
    }
    columns {
      name = "reply_count"
      type = "int"
    }
    columns {
      name = "repost_count"
      type = "int"
    }
    columns {
      name = "passed_filters"
      type = "boolean"
    }
    columns {
      name = "filtered_at"
      type = "string"
    }
    columns {
      name = "filtered_by_func"
      type = "string"
    }
    columns {
      name = "preprocessing_timestamp"
      type = "string"
    }
    columns {
      name = "llm_model_name"
      type = "string"
    }
    columns {
      name = "sociopolitical_was_successfully_labeled"
      type = "boolean"
    }
    columns {
      name = "sociopolitical_reason"
      type = "string"
    }
    columns {
      name = "sociopolitical_label_timestamp"
      type = "string"
    }
    columns {
      name = "is_sociopolitical"
      type = "boolean"
    }
    columns {
      name = "political_ideology_label"
      type = "string"
    }
    columns {
      name = "perspective_was_successfully_labeled"
      type = "boolean"
    }
    columns {
      name = "perspective_reason"
      type = "string"
    }
    columns {
      name = "perspective_label_timestamp"
      type = "string"
    }
    columns {
      name = "prob_toxic"
      type = "double"
    }
    columns {
      name = "prob_severe_toxic"
      type = "double"
    }
    columns {
      name = "prob_identity_attack"
      type = "double"
    }
    columns {
      name = "prob_insult"
      type = "double"
    }
    columns {
      name = "prob_profanity"
      type = "double"
    }
    columns {
      name = "prob_threat"
      type = "double"
    }
    columns {
      name = "prob_affinity"
      type = "double"
    }
    columns {
      name = "prob_compassion"
      type = "double"
    }
    columns {
      name = "prob_constructive"
      type = "double"
    }
    columns {
      name = "prob_curiosity"
      type = "double"
    }
    columns {
      name = "prob_nuance"
      type = "double"
    }
    columns {
      name = "prob_personal_story"
      type = "double"
    }
    columns {
      name = "prob_reasoning"
      type = "double"
    }
    columns {
      name = "prob_respect"
      type = "double"
    }
    columns {
      name = "prob_alienation"
      type = "double"
    }
    columns {
      name = "prob_fearmongering"
      type = "double"
    }
    columns {
      name = "prob_generalization"
      type = "double"
    }
    columns {
      name = "prob_moral_outrage"
      type = "double"
    }
    columns {
      name = "prob_scapegoating"
      type = "double"
    }
    columns {
      name = "prob_sexually_explicit"
      type = "double"
    }
    columns {
      name = "prob_flirtation"
      type = "double"
    }
    columns {
      name = "prob_spam"
      type = "double"
    }
    columns {
      name = "similarity_score"
      type = "double"
    }
    columns {
      name = "most_liked_average_embedding_key"
      type = "string"
    }
    columns {
      name = "consolidation_timestamp"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "custom_feeds" {
  name          = "custom_feeds"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "classification"      = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/custom_feeds/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "custom_feeds_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "user"
      type = "string"
    }

    columns {
      name = "feed"
      type = "array<struct<item:string,score:float>>"
    }

    columns {
      name = "feed_generation_timestamp"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "daily_superposters" {
  name          = "daily_superposters"
  database_name = var.default_glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "classification"      = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/daily_superposters/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "daily_superposters_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "insert_date_timestamp"
      type = "string"
    }
    columns {
      name = "insert_date"
      type = "string"
    }
    columns {
      name = "superposters"
      type = "array<struct<author_did:string,count:int>>"
    }
    columns {
      name = "method"
      type = "string"
    }
    columns {
      name = "top_n_percent"
      type = "float"
    }
    columns {
      name = "threshold"
      type = "int"
    }
  }
}


resource "aws_glue_catalog_table" "user_session_logs" {
  name          = "user_session_logs"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/user_session_logs/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "user_session_logs_json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns {
      name = "user_did"
      type = "string"
    }
    columns {
      name = "cursor"
      type = "string"
    }
    columns {
      name = "limit"
      type = "int"
    }
    columns {
      name = "feed_length"
      type = "int"
    }
    columns {
      name = "feed"
      type = "array<struct<post:string>>"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
  }

  partition_keys {
    name = "bluesky_user_handle"
    type = "string"
  }
}

resource "aws_glue_crawler" "user_session_logs_glue_crawler" {
  name        = "user_session_logs_glue_crawler"
  role        = aws_iam_role.glue_crawler_role.arn
  database_name = var.default_glue_database_name

  s3_target {
    path = "s3://${var.s3_root_bucket_name}/user_session_logs/"
  }

  schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

  configuration = jsonencode({
    "Version" = 1.0,
    "CrawlerOutput" = {
      "Partitions" = {
        "AddOrUpdateBehavior" = "InheritFromTable"
      }
    }
  })
}


resource "aws_glue_crawler" "perspective_api_labels_glue_crawler" {
  name        = "perspective_api_labels_glue_crawler"
  role        = aws_iam_role.glue_crawler_role.arn
  database_name = var.default_glue_database_name

  s3_target {
    path = "s3://bluesky-research/ml_inference_perspective_api/firehose/"
  }

  s3_target {
    path = "s3://bluesky-research/ml_inference_perspective_api/most_liked/"
  }

  schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

  configuration = jsonencode({
    "Version" = 1.0,
    "CrawlerOutput" = {
      "Partitions" = {
        "AddOrUpdateBehavior" = "InheritFromTable"
      }
    }
  })
}


resource "aws_glue_crawler" "llm_sociopolitical_labels_glue_crawler" {
  name        = "llm_sociopolitical_labels_glue_crawler"
  role        = aws_iam_role.glue_crawler_role.arn
  database_name = var.default_glue_database_name

  s3_target {
    path = "s3://${var.s3_root_bucket_name}/ml_inference_sociopolitical/firehose/"
  }

  s3_target {
    path = "s3://${var.s3_root_bucket_name}/ml_inference_sociopolitical/most_liked/"
  }

  schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

  configuration = jsonencode({
    "Version" = 1.0,
    "CrawlerOutput" = {
      "Partitions" = {
        "AddOrUpdateBehavior" = "InheritFromTable"
      }
    }
  })
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
resource "aws_dynamodb_table" "users_whose_social_network_has_been_fetched" {
  name           = "users_whose_social_network_has_been_fetched"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "user_handle"

  attribute {
    name = "user_handle"
    type = "S"
  }

  tags = {
    Name = "users_whose_social_network_has_been_fetched"
  }
}

resource "aws_dynamodb_table" "ml_inference_labeling_sessions" {
  name           = "ml_inference_labeling_sessions"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "inference_timestamp"

  attribute {
    name = "inference_timestamp"
    type = "S"
  }

  attribute {
    name = "inference_type"
    type = "S"
  }

  global_secondary_index {
    name               = "inference_type-index"
    hash_key           = "inference_type"
    projection_type    = "ALL"
  }

  global_secondary_index {
    name               = "inference_timestamp-index"
    hash_key           = "inference_timestamp"
    projection_type    = "ALL"
  }

  tags = {
    Name = "ml_inference_labeling_sessions"
  }
}

resource "aws_dynamodb_table" "vector_embedding_sessions" {
  name           = "vector_embedding_sessions"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "embedding_timestamp"

  attribute {
    name = "embedding_timestamp"
    type = "S"
  }

  tags = {
    Name = "vector_embedding_sessions"
  }
}

resource "aws_dynamodb_table" "enrichment_consolidation_sessions" {
  name           = "enrichment_consolidation_sessions"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "enrichment_consolidation_timestamp"

  attribute {
    name = "enrichment_consolidation_timestamp"
    type = "S"
  }

  tags = {
    Name = "enrichment_consolidation_sessions"
  }
}

resource "aws_dynamodb_table" "rank_score_feed_sessions" {
  name           = "rank_score_feed_sessions"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "feed_generation_timestamp"

  attribute {
    name = "feed_generation_timestamp"
    type = "S"
  }

  tags = {
    Name = "rank_score_feed_sessions"
  }
}

resource "aws_dynamodb_table" "superposter_calculation_sessions" {
  name           = "superposter_calculation_sessions"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "insert_date_timestamp"

  attribute {
    name = "insert_date_timestamp"
    type = "S"
  }

  tags = {
    Name = "superposter_calculation_sessions"
  }
}
