"""Previously, we tracked follower posts in DynamoDB.

We've since moved to tracking only the posts from accounts that study
users follow, not posts that are from accounts who follow study users.

This crawls through S3 in the "in-network-user-activity" folder and deletes
all the follower posts.
"""

pass
