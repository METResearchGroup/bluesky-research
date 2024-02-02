# Bluesky Research


## Installation

This project uses [Anaconda](https://www.anaconda.com/download) for package management. To create a new virtual environment, we can run something like the following (this project uses Python=3.10):

```
conda create -n bluesky-research python=3.10
```

Then, activate the virtual environment:
```
conda activate bluesky-research
```

Finally, install the packages:
```
pip install -r requirements.txt
```

Helpful project links:
- Perspective API: [https://developers.perspectiveapi.com/s/about-the-api-attributes-and-languages?language=en_US](https://developers.perspectiveapi.com/s/about-the-api-attributes-and-languages?language=en_US)

- `atproto` (Python package for Bluesky API): [https://github.com/MarshalX/atproto](https://github.com/MarshalX/atproto)
  - Docs: [https://atproto.blue/en/latest/](https://atproto.blue/en/latest/)
  - `atproto` object defs: [https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/actor/defs.py](https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/actor/defs.py)
  - `atproto` schemas: [https://github.com/bluesky-social/atproto/tree/main/lexicons/app/bsky/feed](https://github.com/bluesky-social/atproto/tree/main/lexicons/app/bsky/feed)

- Bluesky Python code examples
  - https://github.com/kojira/bluesky-chan/tree/main
  - https://github.com/snarfed/lexrpc
  - https://github.com/Bossett/bsky-feeds
  - https://github.com/bluesky-astronomy/astronomy-feeds
  - https://github.com/MarshalX/bluesky-feed-generator
  - https://github.com/whyrusleeping/algoz
  - https://github.com/snarfed/arroba/blob/main/arroba/did.py

[(Community-made) Bluesky new user onboarding](https://from-over-the-horizon.ghost.io/bluesky-social-new-user-guide/)
[DID record lookup](https://web.plc.directory/resolve)

We can possibly inspire our implementation of the recommendation algorithm based on Twitter:
- [Twitter algorithm repo](https://github.com/twitter/the-algorithm)
- [Twitter algorithm press release](https://blog.twitter.com/engineering/en_us/topics/open-source/2023/twitter-recommendation-algorithm)