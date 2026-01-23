"""Experimenting with concurrent request count.

Now that we have an idea of the number of posts that we can put into a prompt
before degradation (5), let's now explore the number of requests that we can do
concurrently.

We're interested in seeing, for a fixed number of posts 'n', the tradeoff between:

1. (Our current approach): run N requests concurrently in parallel.
2. (The proposed new approach): run N / 5 requests, each with a prompt of batch
size 5, in parallel.

Like before, we'll be comparing the runtime and accuracy of the two approaches.
"""

pass
