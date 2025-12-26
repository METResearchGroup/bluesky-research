"""Send daily usage reports about each study user."""

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import smtplib
import tempfile

import pandas as pd

from lib.aws.athena import Athena
from lib.constants import current_datetime_str, timestamp_format
from lib.datetime_utils import calculate_lookback_datetime_str
from lib.helper import LAB_EMAIL_SENDER, LAB_EMAIL_PASSWORD
from services.participant_data.helper import get_all_users

# NOTE: I can store credentials in AWS Secrets Manager
# and then load them from there

# by default, return logs from the previous day
default_lookback_days = 1

athena = Athena()

sender_email = LAB_EMAIL_SENDER
sender_password = LAB_EMAIL_PASSWORD
# TODO: add Billy and Meriel's emails here.
recipient_emails = ["markptorres1@gmail.com", "mindtechnologylab@gmail.com"]
subject = f"({current_datetime_str}): Bluesky app usage metrics for the past 24 hours"


def load_user_session_logs(lookback_days: int = default_lookback_days):
    """Fetches user session logs from Athena."""
    query = """SELECT * FROM user_session_logs"""
    df = athena.query_results_as_df(query)
    df["partition_date"] = pd.to_datetime(
        df["timestamp"], format=timestamp_format
    ).dt.date
    df["partition_date"] = df["partition_date"].apply(lambda x: x.strftime("%Y-%m-%d"))

    lookback_datetime_str = calculate_lookback_datetime_str(lookback_days)

    # get only the logs from the previous day
    df = df[df["partition_date"] >= lookback_datetime_str]
    df = df.sort_values(by=["user_did", "timestamp"], ascending=[True, False])

    # note: maybe only count the ones where cursor != 'eof'? We want, ideally,
    # the unique sessions for each user, and if they hit `eof`, it means
    # that that is the last request in a string of previous requests.
    df = df[df["cursor"] != "eof"]

    # now group by "user_did" and get the counts
    df = (
        df.groupby("user_did")
        .size()
        .reset_index(name="session_count")
        .sort_values(by="session_count", ascending=False)
    )

    # remove any "default" session
    df = df[df["user_did"] != "default"]

    # add date column back in
    df["date"] = lookback_datetime_str

    # now we should have a df with the user_did and the session_count
    return df


def load_user_session_data():
    users = get_all_users()
    user_dicts = [user.dict() for user in users]
    df = pd.DataFrame(user_dicts)
    return df


def send_daily_usage_email(usage_df: pd.DataFrame, recipients: list[str]):
    """Send daily usage email, from the lab email ('mindtechnologylab@gmail.com')."""  # noqa
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_csv_path = os.path.join(temp_dir, f"usage_{current_datetime_str}.csv")
        usage_df.to_csv(temp_csv_path, index=False)

        with open(temp_csv_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= 'usage_{current_datetime_str}.csv'",
            )

            body = f"""
            Hello!

            Here are the Bluesky app usage metrics for the past 24 hours:

            Number of total users: {len(usage_df)}
            Number of total sessions: {usage_df["session_count"].sum()}
            Top 20 users by session count:
            {usage_df[['bluesky_handle', 'session_count']].head(20).to_html()}
            """

            message = MIMEMultipart()
            message["Subject"] = subject
            message["From"] = sender_email
            message["To"] = ",".join(recipients)
            html_part = MIMEText(body)
            message.attach(html_part)
            message.attach(part)

            breakpoint()

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipients, message.as_string())


def main():
    user_data_df: pd.DataFrame = load_user_session_data()
    user_logs_df: pd.DataFrame = load_user_session_logs()
    usage_df: pd.DataFrame = user_logs_df.merge(
        user_data_df,
        left_on="user_did",
        right_on="bluesky_user_did",
        how="inner",
    )
    usage_df = usage_df.sort_values(by="session_count", ascending=False)
    usage_df = usage_df[["bluesky_handle", "user_did", "date", "session_count"]]
    send_daily_usage_email(usage_df=usage_df, recipients=recipient_emails)


if __name__ == "__main__":
    main()
