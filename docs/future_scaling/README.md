# Plans for future scaling

The application represents an end-to-end effort to build a full app to study the impact of algorithmic amplification in a field study of real social media users during the 2024 election. However, due to time and resource constraints, we made some tradeoffs during development.

## Original scope of the project

## What it means to "scale"

1. User scale: add more users ([see SCALE_USERS.md](./SCALE_USERS.md))
2. Data scale: ingesting, storing, compacting, and querying larger volumes of Bluesky data ([see SCALE_DATA.md](./SCALE_DATA.md)).
3. ML scale: (1) running content classifiers and embedding models over larger backfills and fresher data, and (2) adding more ML models ([see SCALE_ML.md](./SCALE_ML.md))
4. Operational scale: adding monitoring, alerting, incident response, CI/CD, secrets management, and formal runbooks ([see SCALE_OPERATIONS.md](./SCALE_OPERATIONS.md)).
5. Product scale: turning the system from a study-specific codebase into a reusable platform for researchers ([see SCALE_PRODUCT.md](./SCALE_PRODUCT.md)).
