/* Week 4*/

/* Number of users who logged on in the fourth week.

Total:
- Onboarded in Week 1:
- Onboarded in Week 2:
*/

/* Total: 1358 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids;

/* Onboarded in Week 1: 1135 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2: 223 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Number of people who logged on for the first time.

Total: 1
Onboarded in Week 1: 1
Onboarded in Week 2: 0
 */

/* Onboarded in Week 1: 1 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_3_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);


/* Onboarded in Week 2: 0 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_3_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);

/* Number of users who logged on every week up to (and including) this week
(including previous weeks) */

/* Onboarded in Week 1: 

Check for two cases:
1. They first logged in Week 1 and then they needed to log in Week 1, 2, and 3.
2. They first logged in on Week 2, so they logged on in Weeks 2 and 3.

Total: 924
*/
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE (
    /* Case 1: First logged in Week 1 */
    (
        user_did IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
        AND user_did IN (SELECT user_did FROM week_3_user_dids)
    )
    OR
    /* Case 2: First logged in Week 2 */
    (
        user_did NOT IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
        AND user_did IN (SELECT user_did FROM week_3_user_dids)
    )
)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2:

Check for two cases:
1. They first logged in on Week 1 (these are for users who I accidentally double-
added to the study, first adding them in Week 1 and then adding them again in Week 2,
though I'll count them in the Week 2 batch).
2. They first logged in on Week 2 (the regular Week 2 batch).

Total: 202
*/
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE (
    /* Case 1: First logged in Week 1 */
    (
        user_did IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
        AND user_did IN (SELECT user_did FROM week_3_user_dids)
    )
    OR
    /* Case 2: First logged in Week 2 */
    (
        user_did NOT IN (SELECT user_did FROM week_1_user_dids)
        AND user_did IN (SELECT user_did FROM week_2_user_dids)
        AND user_did IN (SELECT user_did FROM week_3_user_dids)
    )
)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);


/* Number of users who logged on only this week (not previous weeks) */

/* Onboarded in Week 1: 1 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_3_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_1_onboarded_users);

/* Onboarded in Week 2: 0 */
SELECT COUNT(DISTINCT(user_did))
FROM week_4_user_dids
WHERE user_did NOT IN (SELECT user_did FROM week_1_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_2_user_dids)
AND user_did NOT IN (SELECT user_did FROM week_3_user_dids)
AND user_did IN (SELECT bluesky_user_did FROM week_2_onboarded_users);
