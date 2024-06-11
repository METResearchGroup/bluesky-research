# Pipelines

Maintains the separate pipeline logic for each step.

TODO: the actual notes will be added.

The actual functionality will be handled at the service level, but the pipelines will call the services and manage higher-level things such as orchestration. Each of these can run separately and store their results in a database for downstream services to consume.
