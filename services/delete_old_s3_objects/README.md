# Delete Old S3 Objects Service

This service provides functionality to clean up old objects in S3 buckets by deleting files that are older than 24 hours. While we now have an S3 lifecycle policy in place for automatic TTL (Time To Live) management, this service helps handle objects that were created before the lifecycle policy was implemented.

## Purpose

- Deletes S3 objects older than 24 hours from specified prefixes
- Helps maintain storage hygiene by removing outdated temporary files
- Complements the S3 lifecycle policy for legacy objects

## Prefixes Handled

The service currently manages objects under these prefixes:

- `daily-posts/`
- `athena-results/`
- `sqs-messages/`

## How It Works

1. For each configured prefix, the service:
   - Lists all objects under that prefix
   - Checks the last modified timestamp of each object
   - Deletes objects that are older than 24 hours
   - Logs progress and final deletion counts

## Usage

The service can be run directly through the helper script:

```bash
python -m services.delete_old_s3_objects.helper
```
