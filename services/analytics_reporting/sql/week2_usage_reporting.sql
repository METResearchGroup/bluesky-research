/* Week 2 */

/* number of users who logged on in the second week.

Total: 
Split by when the user was onboarded: 1528
- Onboarded in Week 1: 1254
- Onboarded in Week 2: 262
- Random accounts (filtered out): 12
*/

/* Total: 1528 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids;


/* Onboarded in Week 1: 1254 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2: 262 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* There are DIDs of users who logged in but aren't a part of the onboarded 
users. I must've caught them in the backfill on accident. 

All of these happened to log in on 10/11 for some reason. Strange. But not
remarkable or notable I think.

1:  test-did
2:  did:plc:4o3vlgbsnqahjnetlieg6cef
3:  did:plc:fc6m52a7xbrfm7bkkfdmpyee
4:  did:plc:zf5pmvtzpgiyw3rcliv6megc
5:  did:plc:7kuunarqgknnjjau4td77tya
6:  did:plc:4d67cmbflaaf4p3zz22l5s5b
7:  did:plc:4fen3xtbamnb7trztdddznc2
8:  did:plc:7esvzo6agesonklcdjztl2ym
9:  did:plc:lwjoulvkafxwt6o65iqzhgwn
10: did:plc:3ip2z3bxhrz7xbarlqq2tgqs
11: did:plc:6mpfg47pf7jo4cdbljxcfxat
12: did:plc:ywfdjukgvquoacjmh55rfq5n /* I think this person was part of the study but we removed them */

I don't check to see if the backfill users are in the study (or at least, one
iteration of compaction didn't, though now current backfills do check this), so
this likely is from people who stumbled into the feed.
*/
SELECT * /* 12 rows */
FROM week_2_user_dids w
WHERE w.user_did NOT IN (SELECT bluesky_user_did FROM week_2_onboarded_users)
AND w.user_did NOT IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

SELECT *
FROM week_2_user_dids w
JOIN study_users_with_created_dates s
ON w.user_did = s.bluesky_user_did
WHERE w.user_did NOT IN (SELECT bluesky_user_did FROM week_2_onboarded_users)
AND w.user_did NOT IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Looks like they're indeed from backfill */
SELECT *
FROM user_session_logs
WHERE user_did LIKE '%did:plc:4o3vlgbsnqahjnetlieg6cef%';

/* Total number of users who logged in every week they were in the study, up to
the second week:

Total: 1200
Onboarded in Week 1: 938
    - 938 users logged on both Week 1 and Week 2
Onboarded in Week 2: 262
*/

/* Total: 1528 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids;


/* Onboarded in Week 1: 938 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users)
AND user_did IN (SELECT user_did FROM week_1_user_dids);

/* Onboarded in Week 2: 262 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Number of users who logged on for the first time */
/* Onboarded in Week 1: 316 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);


/* 170 of the users onboarded in Week 2 logged on for the first time in Week 2 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* 262 total users from Week 2 onboarding logged on in Week 2 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* 92 total users from Week 2 onboarding logged on in Week 1??? Bro wut...
OK whatever, I guess. I'll count them as having logged on in Week 1. */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT user_did FROM week_1_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Investigating these users. */
SELECT user_did
FROM week_2_user_dids
WHERE user_did IN (SELECT user_did FROM week_1_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users)
LIMIT 5;

/* OH, I get it. OK, these users were actually onboarded in Week 1, but I
must’ve duplicated the writes on accident in DynamoDB and since DynamoDB
overwrites the data, on the record it logs the most recent update as 10-09,
even though I actually first added them before that.

I'll count these as Week 2 onboarded people. Therefore that makes the count 262.
*/

/* Number of users from Week 1 onboarding who logged in only on Week 2: 316.

Likely that they were onboarded earlier but they were only instructed to join
the study later on.
 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users)
AND user_did NOT IN (SELECT user_did FROM week_1_user_dids);


/* Number of users who dropped off after this week
(logged on up this week, including previous weeks, but not subsequent weeks)
*/

/* Onboarded in Week 1: 60 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did NOT IN (
    SELECT user_did
    FROM week_3_user_dids
) AND user_did NOT IN (
    SELECT user_did
    FROM week_4_user_dids
)
AND user_did IN (
    SELECT bluesky_user_did FROM week_1_onboarded_users
);

/* Onboarded in Week 2: 17 */
SELECT COUNT(DISTINCT(user_did))
FROM week_2_user_dids
WHERE user_did NOT IN (
    SELECT user_did
    FROM week_3_user_dids
) AND user_did NOT IN (
    SELECT user_did
    FROM week_4_user_dids
)
AND user_did IN (
    SELECT bluesky_user_did FROM week_2_onboarded_users
);
