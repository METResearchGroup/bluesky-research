# User Scoring System Experiment

An experimental implementation of a user scoring system for social media platforms, designed to track and display user engagement scores based on their posting activity.

## Overview

This experiment implements a complete user scoring system that:
- Calculates user scores based on post count
- Provides REST API endpoints for accessing user profiles and scores
- Includes a simple web interface for demonstration
- Uses SQLite for data storage (completely independent from production)

## Architecture

The system consists of:
- **Database Layer**: SQLite database with users and posts tables
- **API Layer**: FastAPI application with user endpoints
- **Frontend**: Simple HTML/JavaScript interface for testing
- **Testing**: Comprehensive test suite

## Components

### Database Schema
- `users` table: Stores user profiles with calculated scores
- `posts` table: Stores user posts for score calculation

### API Endpoints
- `GET /user/{handle}` - Get user profile with score
- `GET /user/{handle}/score` - Get just the user's score
- `POST /user/{handle}/recalculate` - Recalculate user score

### Score Calculation
Score = Total number of posts made by the user

## Usage

1. **Setup**: Run `python setup.py` to initialize the database
2. **Start API**: Run `python api_server.py` to start the FastAPI server
3. **Test**: Open `web_demo.html` in a browser to test the interface
4. **Run Tests**: Run `python -m pytest tests/` to execute the test suite

## Files Structure

```
experiments/user_scoring_system/
├── README.md                 # This file
├── setup.py                  # Database initialization
├── models.py                 # Pydantic models
├── database.py               # Database operations
├── api_server.py             # FastAPI application
├── web_demo.html             # Demo web interface
├── sample_data.py            # Sample data for testing
├── tests/
│   ├── test_database.py      # Database tests
│   ├── test_api.py           # API tests
│   └── test_scoring.py       # Score calculation tests
└── requirements.txt          # Dependencies
```

## Testing Details

Tests are located in the `tests/` directory and cover:

- **test_database.py**: Tests database operations
  - User creation and retrieval
  - Post creation and counting
  - Score calculation and caching
  
- **test_api.py**: Tests API endpoints
  - User profile retrieval
  - Score endpoint functionality
  - Error handling and edge cases
  
- **test_scoring.py**: Tests scoring logic
  - Score calculation accuracy
  - Score update mechanisms
  - Performance with large datasets

## Development Notes

This is an experimental implementation designed to:
- Test user scoring concepts without affecting production
- Validate API design patterns
- Experiment with different scoring algorithms
- Provide a sandbox for UI/UX testing