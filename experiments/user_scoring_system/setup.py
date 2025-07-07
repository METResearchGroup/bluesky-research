"""Setup script for the experimental user scoring system.

This script initializes the database and optionally creates sample data
for testing and demonstration purposes.
"""

import argparse
import sys
from pathlib import Path

# Add the current directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

from database import Database
from sample_data import create_sample_data


def setup_database(create_samples: bool = False, num_posts: int = 5) -> None:
    """Set up the database and optionally create sample data.
    
    Args:
        create_samples: Whether to create sample data
        num_posts: Number of posts to create per user
    """
    print("Setting up User Scoring System database...")
    
    # Initialize database
    db = Database()
    print("✓ Database initialized with tables and indexes")
    
    if create_samples:
        print("\nCreating sample data...")
        result = create_sample_data(db, num_posts_per_user=num_posts)
        
        print("\n" + "="*50)
        print("Setup Complete!")
        print("="*50)
        print(f"Users created: {result['users_created']}")
        print(f"Posts created: {result['posts_created']}")
        print(f"Users with updated scores: {result['users_with_updated_scores']}")
        print(f"Total users in database: {result['summary']['total_users']}")
        print(f"Average score: {result['summary']['average_score']:.1f}")
        print(f"Highest score: {result['summary']['highest_score']}")
        print(f"Lowest score: {result['summary']['lowest_score']}")
        
        print("\nSample users and their scores:")
        users = db.get_all_users()
        for user in users:
            print(f"  {user.handle}: {user.score} points ({user.display_name})")
    else:
        print("✓ Database setup complete (no sample data created)")
    
    print("\nNext steps:")
    print("1. Run 'python api_server.py' to start the API server")
    print("2. Open 'web_demo.html' in your browser to test the interface")
    print("3. Run 'python -m pytest tests/' to execute the test suite")


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Set up the User Scoring System database"
    )
    parser.add_argument(
        "--with-samples",
        action="store_true",
        help="Create sample users and posts for testing"
    )
    parser.add_argument(
        "--num-posts",
        type=int,
        default=5,
        help="Number of posts to create per user (default: 5)"
    )
    
    args = parser.parse_args()
    
    try:
        setup_database(
            create_samples=args.with_samples,
            num_posts=args.num_posts
        )
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()