# Consolidate post records

The posts from the firehose have a different format than the posts that come from the feeds (specifically, the posts that come from the feeds have more information given that they have to be fully hydrated with the info needed to display the post on the frontend).

We consolidate those two formats in this service and create a "ConsolidatedPostRecord" object.