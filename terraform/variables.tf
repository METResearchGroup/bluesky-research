variable "aws_region" {
  default = "us-east-2"
}

variable "aws_profile" {
  default = "AWSAdministratorAccess-517478598677"
}

variable "aws_access_key_id" {
  description = "AWS access key ID for role AWSAdministrator"
  type        = string
  default     = "" # set using 'TF_VAR_aws_access_key_id' env var
}

variable "aws_secret_access_key" {
  description = "AWS secret access key for role AWSAdministrator"
  type        = string
  default     = "" # set using 'TF_VAR_aws_secret_access_key' env var
}

variable "s3_root_bucket_name" {
  description = "The name of the S3 bucket"
  type        = string
  default     = "bluesky-research"
}

variable "default_glue_database_name" {
  description = "The name of the default Glue database"
  type        = string
  default     = "default_db"
}

# TODO: change to actual value, not just test one.
variable "bsky_api_lambda_name" {
  description = "The name of the Bluesky API Lambda function"
  type        = string
  default     = "bsky-api-lambda"
}

variable "bsky_api_policy_name" {
  description = "The name of the IAM policy"
  type        = string
  default     = "bsky-api-s3-policy"
}

variable "custom_domain_name" {
  description = "The custom domain name for the API"
  type        = string
  default     = "" # set using 'TF_VAR_custom_domain_name' env var
}


# see https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name
# for env var details in Terraform.
variable "acm_certificate_arn" {
  description = "The ARN of the ACM certificate for the custom domain"
  type        = string
  default     = ""  # set using 'TF_VAR_acm_certificate_arn' env var
}

variable "route53_zone_id" {
  description = "The Route 53 Hosted Zone ID"
  type        = string
  default     = ""  # set using 'TF_VAR_route53_zone_id' env var
}
