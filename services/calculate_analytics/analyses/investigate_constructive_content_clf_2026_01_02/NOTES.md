# Notes

## 2026-01-03

I looked at some visualizations in "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/get_baseline_measures_across_all_posts_2025_09_02/results/visualizations/2025-09-02-08-47-08", comparing the toxicity and constructiveness classifiers, and bruh. Rip. The average toxicity is lower than the average constructiveness, AND a lower proportion of posts are toxic than are constructive.

For moral outrage,  we do see double the weekly average and weekly proportion of moral outrage as compared to constructiveness, so that's directionally good I suppose?

12:52pm: I'm closely analyzing the visualization, and I think I'm seeing the conflict:

- post-election there is a higher marginal effect of EB and DE than in pre-election. So, for example, the engagement algorithm surfaces more constructive content post-election.
- pre-election, both DE and EB have lower effects than the reverse chronological. But post-election, EB has higher marginal effect than reverse chronological while DE breaks even.
- So, EB upranks constructive content? We see that (1) pre-election, EB has a higher marginal effect vs. RC than DE (meaning that EB has higher constructiveness on average?), and (2) post-election, EB has a higher marginal effect still. Across pre-election and post-election, EB seems to uprank constructiveness? But in theory we were supposed to be upranking constructiveness in DE, so why is it that we see higher constructiveness effects in DE than EB?

Some thoughts:

- Our DE penalizes superposters, likely that some big accounts posted more often and so we had more superposters popping up?
  - This helps explains the "why are DE effects more muted than EB", but is still incomplete
- Maybe (1) the average content in EB is more constructive than the average content in RC, which would make sense (can pull up example posts for each), and then (2) the toxicity and superposter penalties are more prevalent than constructiveness.
  - Hmmm... re:(2), if we eliminate toxicity and superposter, shouldn't that leave more room for constructive posts? Only way around this is to argue that us eliminating toxic and superposter content has the after-effect of also removing constructive content?
    - Can see # of posts that are constructive AND from superposters.
    - Can see # of posts that are constructive AND toxic?
  - We can find the number of posts that are toxic, are from a superposter, or are constructive.
  - TBH this should've been tracked in the metadata, bruh. We should've tracked which posts in the feeds were affected by which parts of the scoring algorithm.
  - Somehow we have to make the argument that the posts that were filtered out in the treatment DE algorithm were somewhat more constructive. Luckily the refactored version of the `rank_score_feeds` algo that I created should help clarify the details of the algorithm.
  - The treatment effects were SMALL. So, it's OK to not have a significant result. But we should at least try to be able to explain it.

I think I've captured the key tension as well as some possible next steps? But truly, not sure how to study this closely.

I also wonder if it's worth setting up the Glue tables so that I can run queries in Athena rather than having to run every single query manually in Quest? I'll have to think about that.
