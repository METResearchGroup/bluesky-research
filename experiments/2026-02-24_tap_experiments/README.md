# Tap Experiments: Hydrate Liked Posts

Load ~20 liked post URIs from raw_sync like data, then hydrate them to full post records via the PDS API or Bluesky Tap.

## Run order

1. **Load raw likes** (writes `liked_post_uris.json`):

   ```bash
   uv run python experiments/2026-02-24_tap_experiments/load_raw_likes.py
   ```

   Requires parquet data under `raw_sync/create/like/` for the study date range. If no data exists, create a hand-written `liked_post_uris.json` (JSON array of `at://...` post URIs).

2. **Hydrate via PDS** (writes `hydrated_posts_pds.json`):

   ```bash
   uv run python experiments/2026-02-24_tap_experiments/hydrate_likes.py --method pds
   ```

3. **Hydrate via Tap** (optional; writes `hydrated_posts_tap.json`):

   - Download Tap once (from repo root): `git clone https://github.com/bluesky-social/indigo.git indigo`. Tap lives in [bluesky-social/indigo](https://github.com/bluesky-social/indigo/tree/main/cmd/tap), not atproto.

   - Start Tap in another terminal:

     ```bash
     cd indigo && go run ./cmd/tap run --disable-acks=true
     ```

   - Default Tap HTTP port: `2480`; WebSocket channel: `ws://localhost:2480/channel`.

   - Then run:

     ```bash
     uv run python experiments/2026-02-24_tap_experiments/hydrate_likes.py --method tap
     ```

   - Optional: `--timeout 180` for a longer wait (default 120 seconds).

## Files

- `load_raw_likes.py` – Load raw_sync likes, parse `subject.uri`, write up to 20 liked post URIs to `liked_post_uris.json`.
- `hydrate_likes.py` – Read `liked_post_uris.json`; with `--method pds` call PDS per URI; with `--method tap` use `TapRepoSync` to collect posts from Tap events.
- `tap_client.py` – `TapRepoSync`: `add_repos(dids)`, WebSocket `connect()` / `stream_events()`, `wait_for_posts(post_uris, timeout_sec)`.

## Plan assets

Run logs and notes: `docs/plans/2026-02-24_tap_experiments_7f3a2e/`.
