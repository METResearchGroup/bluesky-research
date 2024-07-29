variable "aws_region" {
  default = "us-east-2"
}

variable "aws_profile" {
  default = "AWSAdministratorAccess-517478598677"
}

variable "s3_root_bucket_name" {
  description = "The name of the S3 bucket"
  type        = string
  default     = "bluesky-research"
}

# TODO: change to actual value, not just test one.
variable "bsky_api_lambda_name" {
  description = "The name of the Bluesky API Lambda function"
  type        = string
  default     = "bsky-api-lambda-test"
}

variable "bsky_api_policy_name" {
  description = "The name of the IAM policy"
  type        = string
  default     = "bsky-api-s3-policy"
}
