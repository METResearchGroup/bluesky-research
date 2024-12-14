"""Send weekly reporting and usage emails.

Based on https://developers.google.com/gmail/api/guides/sending#python
"""

import base64
from datetime import timedelta
from email.message import EmailMessage
import mimetypes
import os

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
import pandas as pd

from lib.aws.athena import Athena
from lib.constants import current_datetime, current_datetime_str, timestamp_format
from services.participant_data.helper import get_all_users


athena = Athena()
current_directory = os.path.dirname(os.path.abspath(__file__))

from_email = "markptorres1@gmail.com"
# TODO: add Billy and Meriel's emails here.
recipient_emails = ["markptorres1@gmail.com", "mindtechnologylab@gmail.com"]
subject = f"({current_datetime_str}): Bluesky app usage metrics for the past 7 days"

default_lookback_days = 7

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
CREDENTIALS_PATH = os.path.join(current_directory, "credentials.json")


def get_gmail_service():
    """Get an authorized Gmail API service instance."""
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = Flow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    print("Successfully authenticated with Gmail API.")
    return build("gmail", "v1", credentials=creds)


def load_user_session_logs(lookback_days: int = default_lookback_days):
    """Fetches user session logs from Athena."""
    lookback_datetime = current_datetime - timedelta(days=lookback_days)
    lookback_datetime_str = lookback_datetime.strftime(timestamp_format)
    query = f"""
    SELECT * FROM user_session_logs
    WHERE partition_date >= '{lookback_datetime_str}'
    """
    df = athena.query_results_as_df(query)
    df["partition_date"] = pd.to_datetime(
        df["timestamp"], format=timestamp_format
    ).dt.date
    df["partition_date"] = df["partition_date"].apply(lambda x: x.strftime("%Y-%m-%d"))

    # TODO: figure out how to get pagination? Maybe the first request by a user
    # within a 5 minute chunk?
    df = df[df["cursor"] != "eof"]

    # remove any "default" session
    df = df[df["user_did"] != "default"]

    # add date column back in
    df["date"] = lookback_datetime_str

    return df


def load_user_session_data():
    users = get_all_users()
    user_dicts = [user.dict() for user in users]
    df = pd.DataFrame(user_dicts)
    return df


def gmail_create_draft_with_attachment():
    """Create and insert a draft email with attachment.
     Print the returned draft's message and id.
    Returns: Draft object, including draft id and message meta data.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    service = get_gmail_service()

    try:
        mime_message = EmailMessage()

        # headers
        mime_message["To"] = ", ".join(recipient_emails)
        mime_message["From"] = from_email
        mime_message["Subject"] = subject

        # text
        mime_message.set_content(
            "Hi, this is automated mail with attachment.Please do not reply."
        )

        # attachment
        attachment_filename = "photo.jpg"
        # guessing the MIME type
        type_subtype, _ = mimetypes.guess_type(attachment_filename)
        maintype, subtype = type_subtype.split("/")

        with open(attachment_filename, "rb") as fp:
            attachment_data = fp.read()
        mime_message.add_attachment(attachment_data, maintype, subtype)

        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()

        create_draft_request_body = {"message": {"raw": encoded_message}}
        # pylint: disable=E1101
        draft = (
            service.users()
            .drafts()
            .create(userId="me", body=create_draft_request_body)
            .execute()
        )
        print(f'Draft id: {draft["id"]}\nDraft message: {draft["message"]}')
    except HttpError as error:
        print(f"An error occurred: {error}")
        draft = None
    return draft


def send_weekly_usage_email(filepath: str):
    """Send weekly usage email, from the lab email ('mindtechnologylab@gmail.com')."""  # noqa
    creds, _ = google.auth.default()
    try:
        # create gmail api client
        service = build("gmail", "v1", credentials=creds)
        mime_message = EmailMessage()

        mime_message["To"] = ", ".join(recipient_emails)
        mime_message["From"] = from_email
        mime_message["Subject"] = subject

        mime_message.set_content(
            "Hi, this is automated mail with attachment.Please do not reply."
        )

        # attachment
        type_subtype, _ = mimetypes.guess_type(filepath)
        maintype, subtype = type_subtype.split("/")

        with open(filepath, "rb") as fp:
            attachment_data = fp.read()
        mime_message.add_attachment(attachment_data, maintype, subtype)

        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()

        create_message = {"raw": encoded_message}
        # pylint: disable=E1101
        send_message = (
            service.users().messages().send(userId="me", body=create_message).execute()
        )
        print(f'Message Id: {send_message["id"]}')

    except HttpError as error:
        print(f"An error occurred: {error}")
        send_message = None

    return send_message


def main():
    # user_data_df: pd.DataFrame = load_user_session_data()
    # user_logs_df: pd.DataFrame = load_user_session_logs()
    # usage_df: pd.DataFrame = user_logs_df.merge(
    #     user_data_df,
    #     left_on="user_did",
    #     right_on="bluesky_user_did",
    #     how="inner",
    # )
    # usage_df = usage_df d.sort_values(by="session_count", ascending=False)
    # usage_df = usage_df[["bluesky_handle", "user_did", "date", "session_count"]]

    test_data = [{"id": "1", "val": 2}, {"id": "2", "val": 3}]
    test_df = pd.DataFrame(test_data)
    usage_df = test_df

    filename = f"weekly_usage_{current_datetime_str}.csv"
    filepath = os.path.join(current_directory, filename)
    usage_df.to_csv(filepath, index=False)

    send_weekly_usage_email(filepath=filepath)

    # send_daily_usage_email(usage_df=usage_df, recipients=recipient_emails)


if __name__ == "__main__":
    main()
