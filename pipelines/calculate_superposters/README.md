# Calculate superposters

We want to calculate, on a given day, the superposters for the day. We'll create a daily list of superposters and store in the DB. When we do the calculation for feeds, we penalize posts that are written by superposters. The penalty for superposters resets per day. If a person was a superposter from posting too much on Thursday, their posts will be downranked on Friday, but they won't be a superposter on Saturday. We'll run this as a daily cron job.
