/* Week 3 */

/* number of users who logged on in the third week.

Total: 
Split by when the user was onboarded: 1470
- Onboarded in Week 1: 1254
- Onboarded in Week 2: 262
- Random accounts (filtered out): 12
*/


/* Total: 1470 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids;

/* Onboarded in Week 1: 1216 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2: 253 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Number of people who logged on for the first time.

Total: 29
Onboarded in Week 1: 20
Onboarded in Week 2: 9
 */

/* Onboarded in Week 1: 20 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);


/* Onboarded in Week 2: 9 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Number of users who logged on every week up to (and including) this week
(including previous weeks) */

/* Onboarded in Week 1: 

Check for two cases:
1. They first logged in Week 1 and then they needed to log in Week 1 and Week 2.
2. They first logged in on Week 2.

Total: 1083
*/
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE (
    /* Case 1: First logged in Week 1 */
    (
        user_did IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
    OR
    /* Case 2: First logged in Week 2 */
    (
        user_did NOT IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2:

Check for two cases:
1. They first logged in on Week 1 (these are for users who I accidentally double-
added to the study, first adding them in Week 1 and then adding them again in Week 2,
though I'll count them in the Week 2 batch).
2. They first logged in on Week 2 (the regular Week 2 batch).

Total: 235
*/
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE (
    /* Case 1: First logged in Week 1 */
    (
        user_did IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
    OR
    /* Case 2: First logged in Week 2 */
    (
        user_did NOT IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);


/* Number of users who logged on only this week (not previous weeks) */

/* Onboarded in Week 1: 20 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2: 9 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Number of users who dropped off after this week
(logged on up this week, including previous weeks, but not subsequent weeks)
*/

/* Onboarded in Week 1: 

Check for two cases:
1. They first logged in Week 1 and then they needed to log in Week 1 and Week 2.
    - Check to see if they logged in Weeks 1 + 2 + 3, but not 4
2. They first logged in on Week 2.
    - Check to see if they logged in Weeks 2 + 3, but not 4

Total: 159
 */
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did NOT IN (
    SELECT user_did
    FROM week_4_user_dids
)
AND user_did IN (
    SELECT bluesky_user_did FROM week_1_onboarded_users
)
AND (
    /* Case 1: First logged in Week 1, needs Weeks 1+2+3 */
    (
        user_did IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
    OR
    /* Case 2: First logged in Week 2, needs Weeks 2+3 */
    (
        user_did NOT IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
);

/* Onboarded in Week 2: 

Check for two cases:
1. They first logged in on Week 1 (these are for users who I accidentally double-
added to the study, first adding them in Week 1 and then adding them again in Week 2,
though I'll count them in the Week 2 batch).
2. They first logged in on Week 2 (the regular Week 2 batch).

Total: 33
*/
SELECT COUNT(DISTINCT(user_did))
FROM week_3_user_dids
WHERE user_did NOT IN (
    SELECT user_did
    FROM week_4_user_dids
)
AND user_did IN (
    SELECT bluesky_user_did FROM week_2_onboarded_users
)
AND (
    /* Case 1: First logged in Week 1, needs Weeks 1+2+3 */
    (
        user_did IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
    OR
    /* Case 2: First logged in Week 2, needs Weeks 2+3 */
    (
        user_did NOT IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
    )
);
