/* Week 1 */

/* number of new users who logged on in the first week.
Since this is the first week if the study, this is indeed the first week
that the Week 1 users have logged on: 1319
*/
SELECT COUNT(DISTINCT(user_did))
FROM week_1_user_dids;
    
/* number of new users who logged in this week who didn't log on (AT ALL) in future weeks: 140 */
SELECT COUNT(DISTINCT(user_did))
FROM week_1_user_dids
WHERE user_did NOT IN (
    SELECT user_did
    FROM week_2_user_dids
) AND user_did NOT IN (
    SELECT user_did
    FROM week_3_user_dids
) AND user_did NOT IN (
    SELECT user_did
    FROM week_4_user_dids
);
