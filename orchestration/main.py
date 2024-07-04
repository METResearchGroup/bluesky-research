"""Logic for orchestration module.

Ideally would use an internal API to trigger each, so that I don't have
to include my entire codebase when building the orchestration image.
"""
from prefect import flow, task


@task
def trigger_latest_data_updates():
    """Triggers consolidate of the latest data updates.

    Triggered after Globus job fetches the latest data from S3.
    """
    pass


@task
def run_perspective_api_inference():
    pass


@task
def get_posts_for_llm_inference():
    pass


@task
def run_llm_inference():
    pass


@task
def trigger_ml_classification_orchestration():
    """Orchestration to trigger individual ML classification tasks."""
    run_perspective_api_inference()
    llm_posts = get_posts_for_llm_inference()
    run_llm_inference(upstream_tasks=[llm_posts])


@task
def trigger_update_latest_user_engagement():
    """Updates latest user new posts, comments, reshares, and likes."""
    pass


@task
def trigger_update_latest_connection_updates():
    """Updates latest follows/followers per user."""
    pass


@task
def trigger_user_activity_updates():
    """Orchestration to trigger individual user engagement tasks."""
    trigger_update_latest_user_engagement()
    trigger_update_latest_connection_updates()


@task
def trigger_get_in_network_posts():
    pass


@task
def trigger_update_superposters():
    pass


@task
def trigger_score_and_create_feeds():
    pass


@task
def trigger_write_feeds_to_s3():
    pass


@flow
def run_pipeline():
    # get upstream data
    get_latest_data_updates = trigger_latest_data_updates()

    # computation, ML, and enrichment
    get_user_activity_updates = trigger_user_activity_updates(
        upstream_tasks=[get_latest_data_updates])
    ml_classification_updates = trigger_ml_classification_orchestration(
        upstream_tasks=[get_latest_data_updates])
    in_network_posts_updates = trigger_get_in_network_posts(
        upstream_tasks=[get_user_activity_updates])
    superposter_updates = trigger_update_superposters(
        upstream_tasks=[get_user_activity_updates])

    # score and create feeds
    score_and_create_feeds = trigger_score_and_create_feeds(
        upstream_tasks=[
            get_latest_data_updates, get_user_activity_updates,
            ml_classification_updates, in_network_posts_updates,
            superposter_updates
        ]
    )

    # write to S3
    trigger_write_feeds_to_s3(
        upstream_tasks=[score_and_create_feeds])


if __name__ == "__main__":
    run_pipeline.serve(name="Bluesky Research (on-prem) Orchestration.")
