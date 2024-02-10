# Manage Bluesky feeds
This service is in charge of maintaining an updated copy of the feeds that will
be presented to Bluesky. This microservice will take the posts from the `feed_postprocessing`
service and write them to the database on a regular schedule in order to make it
available to Bluesky.