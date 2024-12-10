/* Any users who logged on before 9/29 are pilot users */
CREATE OR REPLACE VIEW pilot_user_dids AS
SELECT DISTINCT user_did
FROM user_session_logs
WHERE partition_date < '2024-09-29';

/* Users added to the study, with their creation date. */
CREATE OR REPLACE VIEW study_users_with_created_dates AS
SELECT *,
    SUBSTR(created_timestamp, 1, 10) as date_created
FROM exported_study_users;

/* Test query, to see if the view is valid */
SELECT * FROM study_users_with_created_dates LIMIT 5;

/* Users who were onboarded in the first week: 1920 total. */
CREATE OR REPLACE VIEW week_1_onboarded_users AS (
	SELECT bluesky_handle, bluesky_user_did, date_created
	FROM study_users_with_created_dates
	WHERE date_created <= '2024-10-05'
    AND bluesky_user_did NOT IN (
        SELECT user_did FROM pilot_user_dids
    )
)

/* Users who were onboarded in the second week: 378 total. */
CREATE OR REPLACE VIEW week_2_onboarded_users AS (
	SELECT bluesky_handle, bluesky_user_did, date_created
	FROM study_users_with_created_dates
	WHERE date_created > '2024-10-05'
    AND bluesky_user_did NOT IN (
        SELECT user_did FROM pilot_user_dids
    )
)

/* Users who logged on in the first week. */
CREATE OR REPLACE VIEW week_1_user_dids AS
SELECT DISTINCT user_did
FROM user_session_logs
WHERE partition_date >= '2024-09-29'
AND partition_date <= '2024-10-07'
AND user_did NOT IN (SELECT user_did FROM pilot_user_dids);

/* Users who logged on in the second week. */
CREATE OR REPLACE VIEW week_2_user_dids AS
SELECT DISTINCT user_did
FROM user_session_logs
WHERE partition_date >= '2024-10-08'
AND partition_date <= '2024-10-14'
AND user_did NOT IN (SELECT user_did FROM pilot_user_dids);

/* Users who logged on in the third week. */
CREATE OR REPLACE VIEW week_3_user_dids AS
SELECT DISTINCT user_did
FROM user_session_logs
WHERE partition_date >= '2024-10-15'
AND partition_date <= '2024-10-21'
AND user_did NOT IN (SELECT user_did FROM pilot_user_dids);

/* Users who logged on in the fourth week. */
CREATE OR REPLACE VIEW week_4_user_dids AS
SELECT DISTINCT user_did
FROM user_session_logs
WHERE partition_date >= '2024-10-22'
AND partition_date <= '2024-10-28'
AND user_did NOT IN (SELECT user_did FROM pilot_user_dids);
