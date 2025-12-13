"""Sample data generator for the experimental user scoring system.

This module provides functions to populate the database with sample users
and posts for testing and demonstration purposes.
"""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

from database import Database


def generate_sample_users() -> List[Dict[str, str]]:
    """Generate sample user data.
    
    Returns:
        List[dict]: List of sample user data
    """
    sample_users = [
        {
            "handle": "alice.test",
            "display_name": "Alice Johnson",
            "bio": "Software engineer passionate about AI and machine learning",
            "avatar_url": "https://example.com/avatars/alice.jpg"
        },
        {
            "handle": "bob.dev",
            "display_name": "Bob Smith",
            "bio": "Full-stack developer and tech enthusiast",
            "avatar_url": "https://example.com/avatars/bob.jpg"
        },
        {
            "handle": "charlie.writes",
            "display_name": "Charlie Brown",
            "bio": "Content creator and digital nomad",
            "avatar_url": "https://example.com/avatars/charlie.jpg"
        },
        {
            "handle": "diana.design",
            "display_name": "Diana Prince",
            "bio": "UI/UX designer crafting beautiful experiences",
            "avatar_url": "https://example.com/avatars/diana.jpg"
        },
        {
            "handle": "eve.data",
            "display_name": "Eve Chen",
            "bio": "Data scientist exploring patterns in social media",
            "avatar_url": "https://example.com/avatars/eve.jpg"
        },
        {
            "handle": "frank.mobile",
            "display_name": "Frank Wilson",
            "bio": "Mobile app developer building the future",
            "avatar_url": "https://example.com/avatars/frank.jpg"
        },
        {
            "handle": "grace.security",
            "display_name": "Grace Hopper",
            "bio": "Cybersecurity expert and bug hunter",
            "avatar_url": "https://example.com/avatars/grace.jpg"
        },
        {
            "handle": "henry.startup",
            "display_name": "Henry Ford",
            "bio": "Entrepreneur building the next big thing",
            "avatar_url": "https://example.com/avatars/henry.jpg"
        }
    ]
    
    return sample_users


def generate_sample_posts() -> List[str]:
    """Generate sample post content.
    
    Returns:
        List[str]: List of sample post content
    """
    sample_posts = [
        "Just deployed a new feature! The user scoring system is working great 🚀",
        "Working on some exciting ML experiments today. The results are promising!",
        "Beautiful sunset from my home office. Remote work has its perks ☀️",
        "Debugging is like being a detective in a crime movie where you're also the murderer",
        "Coffee count for today: 4 cups and counting... ☕",
        "Excited to share my latest blog post about distributed systems!",
        "Team meeting went well. Love collaborating with smart people 💡",
        "Just finished reading a fascinating paper on neural networks",
        "Weekend project: building a personal dashboard for tracking habits",
        "The best code is no code at all. Sometimes the simplest solution wins.",
        "Attending a virtual conference today. Learning so much!",
        "Refactored legacy code today. It's like cleaning up after a hurricane.",
        "New keyboard arrived! Time to type at the speed of thought ⌨️",
        "Pair programming session was incredibly productive today",
        "Docker containers make everything so much easier to deploy",
        "Just discovered a new Python library that solves my exact problem!",
        "Code review feedback is a gift. Grateful for thoughtful teammates.",
        "Working late tonight but making great progress on the project",
        "Stack Overflow saved my day once again. What would we do without it?",
        "Celebrating a successful product launch with the team! 🎉",
        "Learning Rust in my spare time. The borrow checker is... interesting.",
        "API design is an art form. Consistency is key.",
        "Just fixed a bug that's been haunting me for weeks. Victory!",
        "Open source contribution merged! Love giving back to the community.",
        "Database optimization reduced query time by 80%. Performance matters!",
        "Microservices architecture is both a blessing and a curse",
        "UI/UX feedback session revealed some great insights",
        "Automated testing caught a critical bug before production. Tests FTW!",
        "Cloud infrastructure is amazing but the bills can be scary 💸",
        "Mentoring junior developers is one of the most rewarding parts of my job"
    ]
    
    return sample_posts


def create_sample_data(db: Database, num_posts_per_user: int = 5) -> Dict[str, Any]:
    """Create sample data in the database.
    
    Args:
        db: Database instance
        num_posts_per_user: Number of posts to create per user
        
    Returns:
        dict: Summary of created data
    """
    users_data = generate_sample_users()
    posts_content = generate_sample_posts()
    
    created_users = []
    created_posts = []
    
    # Create users
    for user_data in users_data:
        try:
            user = db.create_user(
                handle=user_data["handle"],
                display_name=user_data["display_name"],
                bio=user_data["bio"],
                avatar_url=user_data["avatar_url"]
            )
            created_users.append(user)
            print(f"Created user: {user.handle}")
        except ValueError as e:
            print(f"User {user_data['handle']} already exists, skipping...")
    
    # Create posts for each user
    for user in created_users:
        # Randomly select posts for this user
        user_posts = random.sample(posts_content, min(num_posts_per_user, len(posts_content)))
        
        for post_content in user_posts:
            try:
                post = db.create_post(
                    user_id=user.user_id,
                    content=post_content
                )
                created_posts.append(post)
                print(f"Created post for {user.handle}: {post_content[:50]}...")
            except Exception as e:
                print(f"Failed to create post for {user.handle}: {e}")
    
    # Update all user scores
    print("Updating user scores...")
    updated_scores = db.recalculate_all_scores()
    
    return {
        "users_created": len(created_users),
        "posts_created": len(created_posts),
        "users_with_updated_scores": len(updated_scores),
        "summary": {
            "total_users": len(db.get_all_users()),
            "average_score": sum(updated_scores.values()) / len(updated_scores) if updated_scores else 0,
            "highest_score": max(updated_scores.values()) if updated_scores else 0,
            "lowest_score": min(updated_scores.values()) if updated_scores else 0
        }
    }


def main():
    """Main function to create sample data."""
    print("Creating sample data for User Scoring System...")
    
    # Initialize database
    db = Database()
    
    # Create sample data
    result = create_sample_data(db, num_posts_per_user=7)
    
    print("\n" + "="*50)
    print("Sample Data Creation Complete!")
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


if __name__ == "__main__":
    main()