# Tap user likes → Parquet (experiment)

Run notes and Tap tuning for `experiments/tap_experiments_hydrate_likes_2026_03_24/`.

## Prerequisites

- Indigo `cmd/tap` at repo root (`indigo/`), Go toolchain.
- Tap listening: `curl -s http://localhost:2480/health` → `{"status":"ok"}`.

## Example: Tap process

```bash
cd indigo
go run ./cmd/tap run --no-replay --disable-acks=true
```

Optional filters:

```bash
export TAP_COLLECTION_FILTERS=app.bsky.feed.like,app.bsky.feed.post
```

## Example: experiment CLI

From repo root:

```bash
uv run python experiments/tap_experiments_hydrate_likes_2026_03_24/run_tap_user_likes.py \
  --output-dir experiments/tap_experiments_hydrate_likes_2026_03_24/out \
  --timeout-posts 600
```

## First successful run (fill in)

- Date:
- Tap version / indigo commit:
- Counts (stdout): Phase A distinct likes = ___; Phase B posts received = ___
- Notes (timeouts, `/info` quirks, outbox polling):

## Spot-check Parquet

```bash
uv run python -c "import pandas as pd; print(pd.read_parquet('experiments/tap_experiments_hydrate_likes_2026_03_24/out/likes.parquet').head())"
uv run python -c "import pandas as pd; print(pd.read_parquet('experiments/tap_experiments_hydrate_likes_2026_03_24/out/liked_posts.parquet').head())"
```
