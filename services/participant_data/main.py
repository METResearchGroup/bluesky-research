"""Generates the initial tables to store the participant data.

Creates 3 tables:
- study_users: table of onboarding information for each study participant.
- study_questionnaire_data: table of questionnaire data for each study participant.
- user_to_bsky_profile: map of study user to their Bluesky profile.
""" # noqa
from services.participant_data.helper import initialize_study_tables

if __name__ == "__main__":
    initialize_study_tables()
