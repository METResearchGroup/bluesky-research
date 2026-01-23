"""Experimenting with prompt batch size.

We see that there is an inverse trade-off between the prompt batch size and the
quality of performance, especially as it relates to the current concurrent
implementation.

Let's say I have 20 posts:

- In the current approach, we run all 20 posts as 20 requests in parallel.
Then we have to wait for all the 20 requests to finish. After that, we process
them all in one go and then do the next batch. You can imagine that there are
some pain points that would seem to come up. As an example, if you run more
requests in parallel, you run into more edge-case tail latencies. You can also
imagine that there would be, in theory, more errors.
- I had assumed that if we had, instead of doing 20 requests in parallel, that
we can do one request with 20 posts inside of it.

I thought that that could be faster, but now that I think about it, it actually
makes sense why it's not faster. Here are some reasons:

- The runtime and the I/O for the large prompts is going to be larger than any
individual single prompt of the original implementation because there's going
to be more tokens that have to be processed. Therefore, the network latency for
one of the big prompt batch requests should, on average, take longer than the
20 concurrent parallel requests, each of which has only one post.
- Even though we do have 20 requests in parallel, they are kicked up at the
same time. So excluding and barring tail latencies, you generally can assume
that the average completion time for all of the requests would be about the
same time as it would take to complete one request, or the slowest request.
It seems like consistently the slowest request for the scenario of one post in
the query that the tail latency of 20 requests, the slowest request is still
more quick on average than a single large batch prompt. The tail latencies that
come from the network seem to still be quicker than the increase in latency that
you get from the increase in tokens from batching the post into a single prompt.

In this script, our plan is to try a variety of possible batch sizes 'n'. We'll be
comparing two possible approaches:
1. (The current approach): Run 'n' requests in parallel, where 'n' is the number of posts in the batch.
2. (The new approach): Run one request with 'n' posts inside of it.

Right now, our goal is to pin down at what point the tradeoff becomes more
apparent, where the performance of one prompt with n posts inside of it begins
to degrade as compared to having 'n' posts run as 'n' requests in parallel.

I'd like, if possible, to be able to find the sweet spot for the prompt
batch size, where a single request with 'n' posts inside of it performs on
par with having 'n' posts run as 'n' requests in parallel, both from a runtime
and an accuracy perspective.
"""

def run_experiment():
    pass

