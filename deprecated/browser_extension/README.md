# Browser extension

This is where our browser extension will live. For the moment, this is a placeholder folder.

Specs:
- Features:
    - Work for specifically the "bsky.app" website.
    - Blocks:
        - all feeds except the one that we want the user to go on.
        - the list of feeds on the right side of the home page.
    - Records:
        - "dwell time", the time spent on each post.
        - user session time each time they log into the app.
    - Sufficiently secure (need to scpe out what this means).
- Optional:
    - record user activity (e.g., likes, retweets, comments, etc.)
    - the source paths to the pictures of any posts seen by the user (this isn't possible in the API but on the website itself, there are `src` fields in each image that have the CDN path to the image itself).
