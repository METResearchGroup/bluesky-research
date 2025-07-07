"""Database operations for the experimental user scoring system.

This module handles all database interactions including user and post management,
score calculations, and database initialization.
"""

import sqlite3
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

from models import User, Post, UserResponse, ScoreResponse


class Database:
    """Database manager for the user scoring system.
    
    This class handles all database operations including initialization,
    user management, post management, and score calculations.
    """
    
    def __init__(self, db_path: str = "user_scoring.db"):
        """Initialize the database connection.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections.
        
        Yields:
            sqlite3.Connection: Database connection
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
        finally:
            conn.close()
    
    def init_database(self) -> None:
        """Initialize the database with required tables.
        
        Creates the users and posts tables if they don't exist.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    handle TEXT UNIQUE NOT NULL,
                    display_name TEXT NOT NULL,
                    bio TEXT,
                    avatar_url TEXT,
                    score INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create posts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    post_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Create index for faster lookups
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts (user_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_users_handle ON users (handle)
            ''')
            
            conn.commit()
    
    def create_user(self, handle: str, display_name: str, bio: Optional[str] = None, 
                   avatar_url: Optional[str] = None) -> User:
        """Create a new user in the database.
        
        Args:
            handle: User's social media handle
            display_name: User's display name
            bio: Optional bio/description
            avatar_url: Optional avatar URL
            
        Returns:
            User: The created user object
            
        Raises:
            ValueError: If handle already exists
        """
        user_id = str(uuid.uuid4())
        now = datetime.now()
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    INSERT INTO users (user_id, handle, display_name, bio, avatar_url, score, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?)
                ''', (user_id, handle, display_name, bio, avatar_url, now, now))
                conn.commit()
                
                return User(
                    user_id=user_id,
                    handle=handle,
                    display_name=display_name,
                    bio=bio,
                    avatar_url=avatar_url,
                    score=0,
                    created_at=now,
                    updated_at=now
                )
            except sqlite3.IntegrityError:
                raise ValueError(f"User with handle '{handle}' already exists")
    
    def get_user_by_handle(self, handle: str) -> Optional[User]:
        """Get a user by their handle.
        
        Args:
            handle: User's handle (with or without @ prefix)
            
        Returns:
            Optional[User]: User object if found, None otherwise
        """
        # Remove @ prefix if present
        clean_handle = handle.lstrip('@')
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE handle = ?', (clean_handle,))
            row = cursor.fetchone()
            
            if row:
                return User(
                    user_id=row['user_id'],
                    handle=row['handle'],
                    display_name=row['display_name'],
                    bio=row['bio'],
                    avatar_url=row['avatar_url'],
                    score=row['score'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                )
            return None
    
    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by their ID.
        
        Args:
            user_id: User's unique identifier
            
        Returns:
            Optional[User]: User object if found, None otherwise
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            
            if row:
                return User(
                    user_id=row['user_id'],
                    handle=row['handle'],
                    display_name=row['display_name'],
                    bio=row['bio'],
                    avatar_url=row['avatar_url'],
                    score=row['score'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                )
            return None
    
    def create_post(self, user_id: str, content: str) -> Post:
        """Create a new post for a user.
        
        Args:
            user_id: ID of the user creating the post
            content: Text content of the post
            
        Returns:
            Post: The created post object
            
        Raises:
            ValueError: If user doesn't exist
        """
        # Verify user exists
        user = self.get_user_by_id(user_id)
        if not user:
            raise ValueError(f"User with ID '{user_id}' not found")
        
        post_id = str(uuid.uuid4())
        now = datetime.now()
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO posts (post_id, user_id, content, created_at)
                VALUES (?, ?, ?, ?)
            ''', (post_id, user_id, content, now))
            conn.commit()
            
            # Update user's score after creating post
            self.update_user_score(user_id)
            
            return Post(
                post_id=post_id,
                user_id=user_id,
                content=content,
                created_at=now
            )
    
    def get_user_posts(self, user_id: str) -> List[Post]:
        """Get all posts for a user.
        
        Args:
            user_id: User's unique identifier
            
        Returns:
            List[Post]: List of user's posts
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM posts WHERE user_id = ? ORDER BY created_at DESC
            ''', (user_id,))
            rows = cursor.fetchall()
            
            return [
                Post(
                    post_id=row['post_id'],
                    user_id=row['user_id'],
                    content=row['content'],
                    created_at=datetime.fromisoformat(row['created_at'])
                )
                for row in rows
            ]
    
    def get_user_post_count(self, user_id: str) -> int:
        """Get the number of posts for a user.
        
        Args:
            user_id: User's unique identifier
            
        Returns:
            int: Number of posts by the user
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM posts WHERE user_id = ?', (user_id,))
            return cursor.fetchone()[0]
    
    def calculate_user_score(self, user_id: str) -> int:
        """Calculate a user's score based on their posts.
        
        Currently, score = number of posts. This can be enhanced with
        more sophisticated scoring algorithms.
        
        Args:
            user_id: User's unique identifier
            
        Returns:
            int: Calculated score
        """
        return self.get_user_post_count(user_id)
    
    def update_user_score(self, user_id: str) -> int:
        """Update a user's score in the database.
        
        Args:
            user_id: User's unique identifier
            
        Returns:
            int: Updated score
        """
        score = self.calculate_user_score(user_id)
        now = datetime.now()
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET score = ?, updated_at = ? WHERE user_id = ?
            ''', (score, now, user_id))
            conn.commit()
            
        return score
    
    def get_user_profile(self, handle: str) -> Optional[UserResponse]:
        """Get complete user profile with calculated score.
        
        Args:
            handle: User's handle
            
        Returns:
            Optional[UserResponse]: User profile response if found
        """
        user = self.get_user_by_handle(handle)
        if not user:
            return None
        
        post_count = self.get_user_post_count(user.user_id)
        
        return UserResponse(
            user_id=user.user_id,
            handle=user.handle,
            display_name=user.display_name,
            bio=user.bio,
            avatar_url=user.avatar_url,
            score=user.score,
            post_count=post_count,
            created_at=user.created_at,
            updated_at=user.updated_at
        )
    
    def get_user_score(self, handle: str) -> Optional[ScoreResponse]:
        """Get user's score information.
        
        Args:
            handle: User's handle
            
        Returns:
            Optional[ScoreResponse]: Score response if user found
        """
        user = self.get_user_by_handle(handle)
        if not user:
            return None
        
        post_count = self.get_user_post_count(user.user_id)
        
        return ScoreResponse(
            user_id=user.user_id,
            handle=user.handle,
            score=user.score,
            post_count=post_count,
            last_calculated=user.updated_at
        )
    
    def get_all_users(self) -> List[User]:
        """Get all users from the database.
        
        Returns:
            List[User]: List of all users
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users ORDER BY created_at DESC')
            rows = cursor.fetchall()
            
            return [
                User(
                    user_id=row['user_id'],
                    handle=row['handle'],
                    display_name=row['display_name'],
                    bio=row['bio'],
                    avatar_url=row['avatar_url'],
                    score=row['score'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                )
                for row in rows
            ]
    
    def recalculate_all_scores(self) -> Dict[str, int]:
        """Recalculate scores for all users.
        
        Returns:
            Dict[str, int]: Mapping of user_id to updated score
        """
        users = self.get_all_users()
        updated_scores = {}
        
        for user in users:
            score = self.update_user_score(user.user_id)
            updated_scores[user.user_id] = score
        
        return updated_scores