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

# add TTLs
# NOTE: only one lifecycle configuration is allowed, else they'll conflict
# and override each other. See https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_lifecycle_configuration
resource "aws_s3_bucket_lifecycle_configuration" "s3_ttl_lifecycle" {
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

  rule {
    id     = "DeleteSqsMessagesAfterOneDay"
    status = "Enabled"

    filter {
      prefix = "sqs_messages/"
    }

    expiration {
      days = 1
    }
  }

  rule {
    id     = "DeleteQueueMessagesAfterOneDay"
    status = "Enabled"

    filter {
      prefix = "queue_messages/"
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

resource "aws_ecr_repository" "compact_dedupe_data_service" {
  name = "compact_dedupe_data_service"
}

resource "aws_ecr_repository" "consume_sqs_messages_service" {
  name = "consume_sqs_messages_service"
}

resource "aws_ecr_repository" "consolidate_enrichment_integrations_service" {
  name = "consolidate_enrichment_integrations_service"
}

resource "aws_ecr_repository" "feed_api_service" {
  name = "feed_api_service"
}

resource "aws_ecr_repository" "generate_vector_embeddings_service" {
  name = "generate_vector_embeddings_service"
}

resource "aws_ecr_repository" "ml_inference_perspective_api_service" {
  name = "ml_inference_perspective_api_service"
}

resource "aws_ecr_repository" "ml_inference_sociopolitical_service" {
  name = "ml_inference_sociopolitical_service"
}

resource "aws_ecr_repository" "preprocess_raw_data_service" {
  name = "preprocess_raw_data_service"
}

resource "aws_ecr_repository" "rank_score_feeds_service" {
  name = "rank_score_feeds_service"
}

resource "aws_ecr_repository" "sync_firehose_stream_service" {
  name = "sync_firehose_stream_service"
}

resource "aws_ecr_repository" "sync_most_liked_feed_service" {
  name = "sync_most_liked_feed_service"
}

# TODO: get correct AMI.
### EC2 instances ###
resource "aws_instance" "feed_api" {
  ami           = "ami-09efc42336106d2f2" # 64-bit x86
  instance_type = "t3.micro"
  key_name      = "firehoseSyncEc2Key"

  tags = {
    Name = "feed-api-ec2-instance"
  }

  iam_instance_profile = "EC2InstanceProfile"
  vpc_security_group_ids = ["sg-0b3e24638f16807d5"] # references feed_api_sg.
}

# this is the same security group for both the firehose and the Feed API.
resource "aws_security_group" "feed_api_sg" {
  name        = "launch-wizard"
  description = "launch-wizard created 2024-08-12T20:56:38.950Z"
  vpc_id      = "vpc-052fa7eed9a020314"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # port access for FastAPI access.
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Allow access from anywhere (you might want to restrict this)
  }

  # TODO: double-check if this is the correct IPs for API Gateway.
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["3.145.192.0/24", "3.134.64.0/24", "3.134.128.0/24"]  # API Gateway IPs (us-east-2 example)
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    ignore_changes = [name]
  }
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

resource "aws_lambda_function" "sync_most_liked_feed_lambda" {
  function_name = "sync_most_liked_feed_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.sync_most_liked_feed_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 180 # 180 seconds timeout
  memory_size   = 768 # 768 MB of memory. Think that the fasttext inference takes up memory.

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "sync_most_liked_feed_lambda_log_group" {
  name              = "/aws/lambda/sync_most_liked_feed_lambda"
  retention_in_days = 7
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

resource "aws_lambda_function" "consume_sqs_messages_lambda" {
  function_name = "consume_sqs_messages_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.consume_sqs_messages_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 480 # 480 seconds timeout, the lambda can run for 8 minutes.
  memory_size   = 512 # 512 MB of memory

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "consume_sqs_messages_lambda_log_group" {
  name              = "/aws/lambda/consume_sqs_messages_lambda"
  retention_in_days = 7
}

resource "aws_lambda_function" "compact_dedupe_data_lambda" {
  function_name = "compact_dedupe_data_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.compact_dedupe_data_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 480 # 480 seconds timeout, the lambda can run for 8 minutes.
  memory_size   = 1024 # 1024 MB of memory.

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "compact_dedupe_data_lambda_log_group" {
  name              = "/aws/lambda/compact_dedupe_data_lambda"
  retention_in_days = 7
}

resource "aws_lambda_function" "consolidate_enrichment_integrations_lambda" {
  function_name = "consolidate_enrichment_integrations_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.consolidate_enrichment_integrations_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 480 # 480 seconds timeout, the lambda can run for 8 minutes.
  memory_size   = 1024

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "consolidate_enrichment_integrations_lambda_log_group" {
  name              = "/aws/lambda/consolidate_enrichment_integrations_lambda"
  retention_in_days = 7
}


resource "aws_lambda_function" "generate_vector_embeddings_lambda" {
  function_name = "generate_vector_embeddings_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.generate_vector_embeddings_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 480 # 480 seconds timeout, the lambda can run for 8 minutes.
  memory_size   = 512 # 512 MB of memory

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "generate_vector_embeddings_lambda_log_group" {
  name              = "/aws/lambda/generate_vector_embeddings_lambda"
  retention_in_days = 7
}


resource "aws_lambda_function" "ml_inference_perspective_api_lambda" {
  function_name = "ml_inference_perspective_api_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ml_inference_perspective_api_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 480 # 480 seconds timeout, the lambda can run for 8 minutes.
  memory_size   = 1024

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "ml_inference_perspective_api_lambda_log_group" {
  name              = "/aws/lambda/ml_inference_perspective_api_lambda"
  retention_in_days = 7
}

resource "aws_lambda_function" "ml_inference_sociopolitical_lambda" {
  function_name = "ml_inference_sociopolitical_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ml_inference_sociopolitical_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 720 # run for 12 minutes. Looks like I can do 750 posts/6 minutes, so this should support 1500 posts.
  memory_size   = 1024

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "ml_inference_sociopolitical_lambda_log_group" {
  name              = "/aws/lambda/ml_inference_sociopolitical_lambda"
  retention_in_days = 7
}

resource "aws_lambda_function" "rank_score_feeds_lambda" {
  function_name = "rank_score_feeds_lambda"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.rank_score_feeds_service.repository_url}:latest"
  architectures = ["arm64"]
  timeout       = 480 # 480 seconds timeout, the lambda can run for 8 minutes.
  memory_size   = 1024

  lifecycle {
    ignore_changes = [image_uri]
  }
}

resource "aws_cloudwatch_log_group" "rank_score_feeds_lambda_log_group" {
  name              = "/aws/lambda/rank_score_feeds_lambda"
  retention_in_days = 7
}

### Event rules triggers ###

# 24-hour sync for most liked feed.
resource "aws_cloudwatch_event_rule" "sync_most_liked_feed_rule" {
  name                = "sync_most_liked_feed_rule"
  schedule_expression = "cron(0 0 * * ? *)"  # Triggers at 00:00 UTC every day
}

resource "aws_cloudwatch_event_target" "sync_most_liked_feed_target" {
  rule      = aws_cloudwatch_event_rule.sync_most_liked_feed_rule.name
  target_id = "syncMostLikedFeedLambda"
  arn       = aws_lambda_function.sync_most_liked_feed_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_sync_most_liked_feed" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sync_most_liked_feed_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sync_most_liked_feed_rule.arn
}

# Trigger for preprocessing lambda every 45 minutes.
resource "aws_cloudwatch_event_rule" "preprocess_raw_data_event_rule" {
  name                = "preprocess_raw_data_event_rule"
  schedule_expression = "cron(0/45 * * * ? *)"  # Triggers every 45 minutes
}

resource "aws_cloudwatch_event_target" "preprocess_raw_data_event_target" {
  rule      = aws_cloudwatch_event_rule.preprocess_raw_data_event_rule.name
  target_id = "preprocessRawDataLambda"
  arn       = aws_lambda_function.preprocess_raw_data_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_preprocess_raw_data" {
  statement_id  = "AllowExecutionFromCloudWatchPreprocessRawData"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.preprocess_raw_data_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.preprocess_raw_data_event_rule.arn
}

# Trigger for consume_sqs_messages_lambda every 15 minutes.
# resource "aws_cloudwatch_event_rule" "consume_sqs_messages_event_rule" {
#   name                = "consume_sqs_messages_event_rule"
#   schedule_expression = "cron(0/15 * * * ? *)"  # Triggers every 15 minutes
# }

# resource "aws_cloudwatch_event_target" "consume_sqs_messages_event_target" {
#   rule      = aws_cloudwatch_event_rule.consume_sqs_messages_event_rule.name
#   target_id = "consumeSqsMessagesLambda"
#   arn       = aws_lambda_function.consume_sqs_messages_lambda.arn
# }

# resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_consume_sqs_messages" {
#   statement_id  = "AllowExecutionFromCloudWatchConsumeSqsMessages"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.consume_sqs_messages_lambda.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.consume_sqs_messages_event_rule.arn
# }

# Trigger to calculate superposters every 12 hours.
resource "aws_cloudwatch_event_rule" "calculate_superposters_event_rule" {
  name                = "calculate_superposters_event_rule"
  schedule_expression = "cron(0 0/12 * * ? *)"  # Triggers every 12 hours
}

resource "aws_cloudwatch_event_target" "calculate_superposters_event_target" {
  rule      = aws_cloudwatch_event_rule.calculate_superposters_event_rule.name
  target_id = "calculateSuperpostersLambda"
  arn       = aws_lambda_function.calculate_superposters_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_calculate_superposters" {
  statement_id  = "AllowExecutionFromCloudWatchCalculateSuperposters"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.calculate_superposters_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.calculate_superposters_event_rule.arn
}

# Trigger for compact_dedupe_data_lambda every 8 hours.
resource "aws_cloudwatch_event_rule" "compact_dedupe_data_event_rule" {
  name                = "compact_dedupe_data_event_rule"
  schedule_expression = "cron(0 0/8 * * ? *)"  # Triggers every 8 hours
}

resource "aws_cloudwatch_event_target" "compact_dedupe_data_event_target" {
  rule      = aws_cloudwatch_event_rule.compact_dedupe_data_event_rule.name
  target_id = "compactDedupeDataLambda"
  arn       = aws_lambda_function.compact_dedupe_data_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_compact_dedupe_data" {
  statement_id  = "AllowExecutionFromCloudWatchCompactDedupeData"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.compact_dedupe_data_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.compact_dedupe_data_event_rule.arn
}

# Trigger ML lambdas every 4 hours.
resource "aws_cloudwatch_event_rule" "perspective_api_event_rule" {
  name                = "perspective_api_event_rule"
  schedule_expression = "cron(0 0/4 * * ? *)"  # Triggers every 4 hours
}

resource "aws_cloudwatch_event_target" "perspective_api_event_target" {
  rule      = aws_cloudwatch_event_rule.perspective_api_event_rule.name
  target_id = "perspectiveApiLambda"
  arn       = aws_lambda_function.ml_inference_perspective_api_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_perspective_api" {
  statement_id  = "AllowExecutionFromCloudWatchPerspectiveAPI"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ml_inference_perspective_api_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.perspective_api_event_rule.arn
}

resource "aws_cloudwatch_event_rule" "sociopolitical_event_rule" {
  name                = "sociopolitical_event_rule"
  schedule_expression = "cron(0 0/4 * * ? *)"  # Triggers every 4 hours
}

resource "aws_cloudwatch_event_target" "sociopolitical_event_target" {
  rule      = aws_cloudwatch_event_rule.sociopolitical_event_rule.name
  target_id = "sociopoliticalLambda"
  arn       = aws_lambda_function.ml_inference_sociopolitical_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_sociopolitical" {
  statement_id  = "AllowExecutionFromCloudWatchSociopolitical"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ml_inference_sociopolitical_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sociopolitical_event_rule.arn
}

# consolidate enrichment integrations every 4 hours, starting at the 20 minute mark.
# this is run offset of the ML lambdas to avoid race conditions where
# the ML lambdas are still running and modifying the data.
resource "aws_cloudwatch_event_rule" "consolidate_enrichment_integrations_event_rule" {
  name                = "consolidate_enrichment_integrations_event_rule"
  schedule_expression = "cron(20 0/4 * * ? *)"  # Triggers every 4 hours, starting at 20 minutes past the hour
}

resource "aws_cloudwatch_event_target" "consolidate_enrichment_integrations_event_target" {
  rule      = aws_cloudwatch_event_rule.consolidate_enrichment_integrations_event_rule.name
  target_id = "consolidateEnrichmentIntegrationsLambda"
  arn       = aws_lambda_function.consolidate_enrichment_integrations_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_consolidate_enrichment_integrations" {
  statement_id  = "AllowExecutionFromCloudWatchConsolidateEnrichmentIntegrations"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.consolidate_enrichment_integrations_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.consolidate_enrichment_integrations_event_rule.arn
}

# Rank score feeds every 8 hours, starting at 45 minutes past the hour.
resource "aws_cloudwatch_event_rule" "rank_score_feeds_event_rule" {
  name                = "rank_score_feeds_event_rule"
  schedule_expression = "cron(45 0/8 * * ? *)"  # Triggers every 8 hours, 45 minutes past the hour
}

resource "aws_cloudwatch_event_target" "rank_score_feeds_event_target" {
  rule      = aws_cloudwatch_event_rule.rank_score_feeds_event_rule.name
  target_id = "rankScoreFeedsLambda"
  arn       = aws_lambda_function.rank_score_feeds_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_invoke_rank_score_feeds" {
  statement_id  = "AllowExecutionFromCloudWatchRankScoreFeeds"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.rank_score_feeds_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.rank_score_feeds_event_rule.arn
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
  path_part   = "test"
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
    aws_api_gateway_integration.bluesky_feed_api_root_integration,
    aws_api_gateway_integration.bluesky_ec2_feed_api_integration
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

## EC2 instance API ##
resource "aws_api_gateway_resource" "bluesky_ec2_feed_api" {
  rest_api_id = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  parent_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.root_resource_id
  path_part   = "{proxy+}"
}

resource "aws_api_gateway_method" "bluesky_ec2_feed_api_method" {
  rest_api_id   = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  resource_id   = aws_api_gateway_resource.bluesky_ec2_feed_api.id
  http_method   = "ANY"
  authorization = "NONE"

  request_parameters = {
    "method.request.path.proxy" = true
  }
}

# This resource defines an API Gateway method.
# allowing HTTP GET requests without any authorization.
# It serves as an entry point for clients to access the API,
# enabling them to retrieve data or perform actions defined in the backend.
resource "aws_api_gateway_integration" "bluesky_ec2_feed_api_integration" {
  rest_api_id = aws_api_gateway_rest_api.bluesky_feed_api_gateway.id
  resource_id = aws_api_gateway_resource.bluesky_ec2_feed_api.id
  http_method = aws_api_gateway_method.bluesky_ec2_feed_api_method.http_method

  type                    = "HTTP_PROXY"
  integration_http_method = "ANY"
  uri                     = "http://${aws_instance.feed_api.public_dns}:8000/{proxy}"

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }

  connection_type = "INTERNET"
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
          "s3:DeleteObject",
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
          "s3:PutObjectAcl",
          "s3:*"
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
          "logs:*"
        ],
        Effect   = "Allow",
        Resource = "*"
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
          "glue:*"
        ],
        Effect   = "Allow",
        Resource = "*"
      },
      # Add Athena policy for accessing Athena tables
      {
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:*"
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
      # get ECR permissions
      {
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:GetRepositoryPolicy",
          "ecr:DescribeRepositories",
          "ecr:ListImages",
          "ecr:DescribeImages",
          "ecr:BatchGetImage"
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

# Glue table for firehose sync posts
resource "aws_glue_catalog_table" "firehose_sync_posts" {
  database_name = aws_glue_catalog_database.default.name
  name          = "study_user_firehose_sync_posts"

  storage_descriptor {
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

    location      = "s3://${var.s3_root_bucket_name}/study_user_activity/create/post/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }

  table_type = "EXTERNAL_TABLE"
}

# Glue table for in-network user posts.
resource "aws_glue_catalog_table" "in_network_firehose_sync_posts" {
  database_name = aws_glue_catalog_database.default.name
  name          = "in_network_firehose_sync_posts"

  storage_descriptor {
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

    location      = "s3://${var.s3_root_bucket_name}/in_network_user_activity/create/post/"
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

resource "aws_glue_catalog_table" "preprocessed_posts" {
  database_name = aws_glue_catalog_database.default.name
  name          = "preprocessed_posts"

  table_type = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/preprocessed_data/preprocessed_posts/"
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

  # partition_keys {
  #   name = "source"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "year"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "month"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "day"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "hour"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "minute"
  #   type = "string"
  # }
}

resource "aws_glue_catalog_table" "queue_messages" {
  database_name = aws_glue_catalog_database.default.name
  name          = "queue_messages"

  table_type = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/queue_messages/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "JsonSerDe"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "insert_timestamp"
      type = "string"
    }
    columns {
      name = "data"
      type = "struct<sync:struct<source:string,operation:string,operation_type:string,s3_keys:array<string>>>"
    }
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
          "s3:*"
        ],
        Resource = [
          "arn:aws:s3:::bluesky-research",
          "arn:aws:s3:::bluesky-research/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "glue:*"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "logs:*",
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

# resource "aws_glue_crawler" "preprocessed_posts_crawler" {
#   name        = "preprocessed_posts_crawler"
#   role        = aws_iam_role.glue_crawler_role.arn
#   database_name = var.default_glue_database_name

#   s3_target {
#     path = "s3://${var.s3_root_bucket_name}/preprocessed_data/preprocessed_posts/"
#   }

#   schedule = "cron(0 */8 * * ? *)"  # Every 8 hours

#   configuration = jsonencode({
#     "Version" = 1.0,
#     "CrawlerOutput" = {
#       Partitions = { AddOrUpdateBehavior = "InheritFromTable" } # prevents crawler from changing schema: https://docs.aws.amazon.com/glue/latest/dg/crawler-schema-changes-prevent.html
#       Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
#     }
#     Grouping = {
#       TableGroupingPolicy = "CombineCompatibleSchemas"
#     }
#   })

#   schema_change_policy {
#     delete_behavior = "LOG"
#     update_behavior = "UPDATE_IN_DATABASE"
#   }
# }

resource "aws_glue_catalog_table" "perspective_api_labels" {
  name          = "ml_inference_perspective_api"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
    "compressionType" = "none"
  }

  storage_descriptor {
    location      = "s3://bluesky-research/ml_inference_perspective_api/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "perspective_api_labels_json"
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
  }

  # partition_keys {
  #   name = "source"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "year"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "month"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "day"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "hour"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "minute"
  #   type = "string"
  # }
}

resource "aws_glue_catalog_table" "llm_sociopolitical_labels" {
  name          = "ml_inference_sociopolitical"
  database_name = var.default_glue_database_name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
    "compressionType" = "none"
  }

  storage_descriptor {
    location      = "s3://${var.s3_root_bucket_name}/ml_inference_sociopolitical/"
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

  # partition_keys {
  #   name = "source"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "year"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "month"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "day"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "hour"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "minute"
  #   type = "string"
  # }
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
    "compressionType" = "none"
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

  # partition_keys {
  #   name = "bluesky_user_handle"
  #   type = "string"
  # }
}

# resource "aws_glue_catalog_table" "sqs_messages" {
#   name          = "sqs_messages"
#   database_name = var.default_glue_database_name
#   table_type    = "EXTERNAL_TABLE"

#   parameters = {
#     "classification" = "json"
#   }

#   storage_descriptor {
#     location      = "s3://${var.s3_root_bucket_name}/sqs_messages/"
#     input_format  = "org.apache.hadoop.mapred.TextInputFormat"
#     output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

#     ser_de_info {
#       name                  = "sqs_messages_json"
#       serialization_library = "org.openx.data.jsonserde.JsonSerDe"
#     }
#     columns {
#       name = "source"
#       type = "string"
#     }
#     columns {
#       name = "insert_timestamp"
#       type = "string"
#     }
#     columns {
#       name = "data"
#       type = "string"
#     }
#   }

#   partition_keys {
#     name = "queue_name"
#     type = "string"
#   }
#   partition_keys {
#     name = "year"
#     type = "int"
#   }
#   partition_keys {
#     name = "month"
#     type = "int"
#   }
#   partition_keys {
#     name = "day"
#     type = "int"
#   }
#   partition_keys {
#     name = "hour"
#     type = "int"
#   }
#   partition_keys {
#     name = "minute"
#     type = "int"
#   }
# }

# resource "aws_glue_crawler" "user_session_logs_glue_crawler" {
#   name        = "user_session_logs_glue_crawler"
#   role        = aws_iam_role.glue_crawler_role.arn
#   database_name = var.default_glue_database_name

#   s3_target {
#     path = "s3://${var.s3_root_bucket_name}/user_session_logs/"
#   }

#   schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

#   configuration = jsonencode({
#     "Version" = 1.0,
#     "CrawlerOutput" = {
#       Partitions = { AddOrUpdateBehavior = "InheritFromTable" } # prevents crawler from changing schema: https://docs.aws.amazon.com/glue/latest/dg/crawler-schema-changes-prevent.html
#       Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
#     }
#     Grouping = {
#       TableGroupingPolicy = "CombineCompatibleSchemas"
#     }
#   })

#   schema_change_policy {
#     delete_behavior = "LOG"
#     update_behavior = "UPDATE_IN_DATABASE"
#   }

#   # Error: updating Glue Crawler (user_session_logs_glue_crawler): InvalidInputException: The SchemaChangePolicy for "Crawl new folders only" Amazon S3 target can have only LOG DeleteBehavior value and LOG UpdateBehavior value.
#   # # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_crawler#recrawl_behavior
#   # # https://geeks.wego.com/creating-glue-crawlers-via-terraform/
#   # recrawl_policy  {
#   #   recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
#   # }
# }


# resource "aws_glue_crawler" "perspective_api_labels_glue_crawler" {
#   name        = "perspective_api_labels_glue_crawler"
#   role        = aws_iam_role.glue_crawler_role.arn
#   database_name = var.default_glue_database_name

#   s3_target {
#     path = "s3://${var.s3_root_bucket_name}/ml_inference_perspective_api/"
#   }

#   schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

#   configuration = jsonencode({
#     "Version" = 1.0,
#     "CrawlerOutput" = {
#       Partitions = { AddOrUpdateBehavior = "InheritFromTable" } # prevents crawler from changing schema: https://docs.aws.amazon.com/glue/latest/dg/crawler-schema-changes-prevent.html
#       Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
#     }
#     Grouping = {
#       TableGroupingPolicy = "CombineCompatibleSchemas"
#     }
#   })

#   schema_change_policy {
#     delete_behavior = "LOG"
#     update_behavior = "UPDATE_IN_DATABASE"
#   }
# }


# resource "aws_glue_crawler" "llm_sociopolitical_labels_glue_crawler" {
#   name        = "llm_sociopolitical_labels_glue_crawler"
#   role        = aws_iam_role.glue_crawler_role.arn
#   database_name = var.default_glue_database_name

#   s3_target {
#     path = "s3://${var.s3_root_bucket_name}/ml_inference_sociopolitical/"
#   }

#   schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

#   configuration = jsonencode({
#     "Version" = 1.0,
#     "CrawlerOutput" = {
#       Partitions = { AddOrUpdateBehavior = "InheritFromTable" } # prevents crawler from changing schema: https://docs.aws.amazon.com/glue/latest/dg/crawler-schema-changes-prevent.html
#       Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
#     }
#     Grouping = {
#       TableGroupingPolicy = "CombineCompatibleSchemas"
#     }
#   })

#   schema_change_policy {
#     delete_behavior = "LOG"
#     update_behavior = "UPDATE_IN_DATABASE"
#   }
# }

# resource "aws_glue_crawler" "queue_messages_crawler" {
#   name        = "queue_messages_crawler"
#   role        = aws_iam_role.glue_crawler_role.arn
#   database_name = var.default_glue_database_name

#   s3_target {
#     path = "s3://${var.s3_root_bucket_name}/queue_messages/"
#   }

#   schedule = "cron(0 */6 * * ? *)"  # Every 6 hours

#   configuration = jsonencode({
#     "Version" = 1.0,
#     "CrawlerOutput" = {
#       "Partitions" = {
#         "AddOrUpdateBehavior" = "InheritFromTable"
#       }
#     }
#   })
# }

resource "aws_cloudwatch_log_group" "glue_crawler_logs" {
  name              = "/aws-glue/crawlers"
  retention_in_days = 14  # Retain logs for 14 days
}

resource "aws_cloudwatch_log_stream" "llm_sociopolitical_labels_crawler_stream" {
  log_group_name = aws_cloudwatch_log_group.glue_crawler_logs.name
  name           = "llm_sociopolitical_labels_crawler_stream"
}

resource "aws_cloudwatch_log_stream" "perspective_api_labels_crawler_stream" {
  log_group_name = aws_cloudwatch_log_group.glue_crawler_logs.name
  name           = "perspective_api_labels_crawler_stream"
}

resource "aws_cloudwatch_log_stream" "preprocessed_posts_crawler_stream" {
  log_group_name = aws_cloudwatch_log_group.glue_crawler_logs.name
  name           = "preprocessed_posts_crawler_stream"
}

# resource "aws_cloudwatch_log_stream" "queue_messages_crawler_stream" {
#   log_group_name = aws_cloudwatch_log_group.glue_crawler_logs.name
#   name           = "queue_messages_crawler_stream"
# }

# resource "aws_cloudwatch_log_stream" "user_session_logs_crawler_stream" {
#   log_group_name = aws_cloudwatch_log_group.glue_crawler_logs.name
#   name           = "user_session_logs_crawler_stream"
# }

# Log group.
resource "aws_cloudwatch_log_group" "sync_firehose_logs" {
  name = "sync-firehose-logs"
  retention_in_days = 5
}

# set up alert if there's been no logs for 30 hours.
resource "aws_cloudwatch_log_metric_filter" "no_logs_filter" {
  name           = "NoLogsFilter"
  pattern        = ""
  log_group_name = aws_cloudwatch_log_group.sync_firehose_logs.name

  metric_transformation {
    name      = "EventCount"
    namespace = "SyncFirehoseMetrics"
    value     = "1"
  }
}

resource "aws_sns_topic" "sync_firehose_alerts" {
  name = "sync-firehose-alerts"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.sync_firehose_alerts.arn
  protocol  = "email"
  endpoint  = "markptorres1@gmail.com"
}

resource "aws_cloudwatch_metric_alarm" "no_logs_alarm" {
  alarm_name          = "NoLogsReceived"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "EventCount"
  namespace           = "SyncFirehoseMetrics"
  period              = "1800"  # 30 minutes
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "This alarm goes off if no logs are received in 30 minutes"
  actions_enabled     = true
  alarm_actions       = [aws_sns_topic.sync_firehose_alerts.arn]
  treat_missing_data  = "breaching"

  dimensions = {
    LogGroupName = aws_cloudwatch_log_group.sync_firehose_logs.name
  }
}

### Alerting for lambda failures ###
resource "aws_sns_topic" "lambda_alerts" {
  name = "lambda-failure-alerts"
}

# SNS Topic Subscription
resource "aws_sns_topic_subscription" "lambda_alerts_email" {
  topic_arn = aws_sns_topic.lambda_alerts.arn
  protocol  = "email"
  endpoint  = "markptorres1@gmail.com"
}

# CloudWatch Alarm for Lambda errors
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = toset([
    "sync_most_liked_feed_lambda",
    "preprocess_raw_data_lambda",
    "calculate_superposters_lambda",
    "compact_dedupe_data_lambda",
    "ml_inference_perspective_api_lambda",
    "ml_inference_sociopolitical_lambda",
    "consolidate_enrichment_integrations_lambda",
    "rank_score_feeds_lambda",
  ])

  alarm_name          = "Lambda-Errors-${each.key}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "60"  # 1 minute
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This alarm monitors for any errors in the Lambda function ${each.key}"
  alarm_actions       = [aws_sns_topic.lambda_alerts.arn]

  dimensions = {
    FunctionName = each.key
  }
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "lambda_errors_dashboard" {
  dashboard_name = "LambdaErrorsDashboard"

  dashboard_body = jsonencode({
    widgets = [
      for lambda_name in [
        "sync_most_liked_feed_lambda",
        "preprocess_raw_data_lambda",
        "calculate_superposters_lambda",
        "compact_dedupe_data_lambda",
        "ml_inference_perspective_api_lambda",
        "ml_inference_sociopolitical_lambda",
        "consolidate_enrichment_integrations_lambda",
        "rank_score_feeds_lambda",
      ] : {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", lambda_name]
          ]
          view    = "timeSeries"
          stacked = false
          region  = "us-east-1"  # replace with your region
          title   = "Errors for ${lambda_name}"
        }
      }
    ]
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
