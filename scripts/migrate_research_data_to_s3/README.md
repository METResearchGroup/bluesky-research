# Migrate research data to S3

Our research data from our Bluesky Nature paper is, as of the time of writing (November 2025), 1 year old. We want to repurpose this pipeline to be more generalized, so to support refactoring and possibly breaking API changes, we're TTLing the study data to S3. This code will support that.
