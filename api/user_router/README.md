# User API Router

The User API Router provides endpoints for accessing user profiles and scores in the Bluesky Research Platform.

## Overview

This API allows you to retrieve user profile information including a dynamically calculated score based on the number of posts a user has made. User scores are calculated in real-time and cached in the database for performance.

## Endpoints

### GET /user/{handle}

Retrieves the complete user profile including their current score.

**Parameters:**
- `handle` (string): The user's Bluesky handle (with or without @ prefix)

**Response:**
```json
{
  "study_user_id": "abc123...",
  "bluesky_handle": "username.bsky.social",
  "bluesky_user_did": "did:plc:xyz789...",
  "condition": "engagement",
  "score": 42,
  "is_study_user": true,
  "created_timestamp": "2025-01-01T00:00:00Z"
}
```

**Example:**
```bash
curl -X GET "http://localhost:8080/user/alice.bsky.social"
curl -X GET "http://localhost:8080/user/@alice.bsky.social"  # @ prefix is automatically stripped
```

### GET /user/{handle}/score

Retrieves just the user's score information.

**Parameters:**
- `handle` (string): The user's Bluesky handle (with or without @ prefix)

**Response:**
```json
{
  "bluesky_handle": "username.bsky.social",
  "score": 42
}
```

**Example:**
```bash
curl -X GET "http://localhost:8080/user/alice.bsky.social/score"
```

## Score Calculation

User scores are calculated based on the number of posts a user has made in the study. The scoring logic:

1. **Score = Number of Posts**: Each post contributes 1 point to the user's score
2. **Real-time Calculation**: Scores are calculated on-demand when requested
3. **Database Caching**: Calculated scores are cached in the database to improve performance
4. **Automatic Updates**: Scores are automatically updated if they differ from the cached value

## Error Handling

### 404 Not Found
Returned when a user with the specified handle is not found in the study.

```json
{
  "detail": "User with handle 'nonexistent.bsky.social' not found"
}
```

### 500 Internal Server Error
Returned when there's a server-side error (database issues, etc.).

```json
{
  "detail": "Internal server error while retrieving user profile"
}
```

## Implementation Details

### Data Sources

- **User Data**: Retrieved from DynamoDB table "study_participants"
- **Post Data**: Retrieved from SQLite table "PostsWrittenByStudyUsers"

### Performance Optimizations

1. **Lazy Loading**: Scores are only calculated when requested
2. **Database Caching**: Calculated scores are stored in the database
3. **Differential Updates**: Database is only updated when scores change
4. **Error Resilience**: Returns score of 0 if post counting fails

### Database Schema

The user model includes the following fields:
- `study_user_id`: Unique identifier for the user in the study
- `bluesky_handle`: User's Bluesky handle
- `bluesky_user_did`: User's decentralized identifier
- `condition`: Experimental condition the user is in
- `score`: User's calculated score (new field)
- `is_study_user`: Whether this is a study participant or test user
- `created_timestamp`: When the user was added to the study

## Testing

The user scoring functionality includes comprehensive tests:

### Unit Tests
- Score calculation logic
- Database operations
- Error handling
- User retrieval by handle

### Integration Tests
- API endpoint functionality
- End-to-end user flows
- Error response handling

### Running Tests

```bash
# Run user scoring tests
pytest services/participant_data/tests/test_user_scoring.py

# Run API endpoint tests
pytest api/user_router/tests/test_user_routes.py

# Run all tests
pytest
```

## API Documentation

Interactive API documentation is available at `/docs` when the server is running.

## Examples

### Basic Usage

```python
import requests

# Get full user profile
response = requests.get("http://localhost:8080/user/alice.bsky.social")
user_profile = response.json()
print(f"User {user_profile['bluesky_handle']} has score: {user_profile['score']}")

# Get just the score
response = requests.get("http://localhost:8080/user/alice.bsky.social/score")
score_data = response.json()
print(f"User score: {score_data['score']}")
```

### Handling Errors

```python
import requests

try:
    response = requests.get("http://localhost:8080/user/nonexistent.bsky.social")
    response.raise_for_status()
    user_profile = response.json()
except requests.exceptions.HTTPError as e:
    if response.status_code == 404:
        print("User not found")
    else:
        print(f"Error: {response.json()['detail']}")
```

## Future Enhancements

Potential improvements to the user scoring system:

1. **Weighted Scoring**: Different post types could have different point values
2. **Time-based Scoring**: More recent posts could be worth more points
3. **Engagement Scoring**: Include likes, reposts, and replies in score calculation
4. **Batch Updates**: Periodic batch updates of all user scores
5. **Score History**: Track score changes over time