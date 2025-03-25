# Backfill Ticket Steps

1. Get backfill to work

2. Figure out what the data looks like
   - Core data types to focus on:
     - Posts
       - Posts whose author is in the study
       - Posts that are reshared/retweeted by an author in the study
       - Posts that are replies to another post, where the author of the reply is a user in the study
     - Likes
       - Record both the like record itself and the post that was liked
     - Follows
       - Save the record of the follow

3. Get the pipeline set up to run on Quest once it's back up
   - The service should accept a list of users and a list of typefaces of data to sync

4. Complete backfill, preprocess and label all data
   - Step 1: Add to preprocessing pipeline
   - Step 2: Add to integration pipelines
   - Step 3: Record all backfilled data from syncs into a separate folder called study_user_activity, while integrating the actual posts into the typical pre-processing pipeline, enabling joins against the study_user_activity user activity records against the base pool of records in the pre-process posts

5. Deal with likes and backfill the posts that were liked
   - Repeat all the above steps but for the authors of the posts that were liked