# Tap experiments verification notes

## Manual verification (2026-02-24)

### 1. Data exists
- Raw sync like parquet present under project data path; `load_data_from_local_storage` with `record_type="like"` returned data.

### 2. Run load script
- **Command:** `uv run python experiments/2026-02-24_tap_experiments/load_raw_likes.py`
- **Result:** Success. Output: "Wrote 20 liked post URIs to .../liked_post_uris.json".
- **Check:** `experiments/2026-02-24_tap_experiments/liked_post_uris.json` exists and is a JSON array of 20 strings (at://... URIs).

### 3. Run hydrate (PDS)
- **Command:** `uv run python experiments/2026-02-24_tap_experiments/hydrate_likes.py --method pds`
- **Result:** Success. Output: "Wrote 16 hydrated posts to .../hydrated_posts_pds.json".
- **Note:** 4 of 20 URIs failed (repo not found or record not found via PDS), which is expected for older/deleted content. Script correctly skips failed lookups and writes only successful records.

### 4. Run Tap
- **Download:** From repo root, `git clone https://github.com/bluesky-social/indigo.git indigo` (Tap lives in indigo, not atproto).
- **Start Tap (fresh DB):** `cd indigo && go run ./cmd/tap run --disable-acks=true`. Default: port 2480, SQLite at `./tap.db`. For a clean run, remove `indigo/tap.db` if it already had the DIDs (otherwise no new backfill events).
- **Run hydrate:** `uv run python experiments/2026-02-24_tap_experiments/hydrate_likes.py --method tap --timeout 300`
- **Result:** Success. Wrote 16 hydrated posts to `hydrated_posts_tap.json` (16 of 20; 4 same as PDS not found). Tap client was updated to: (1) connect to WebSocket before calling `add_repos` so backfill events are not missed, (2) parse indigo Tap event format (`type: "record"`, `record.did`/`collection`/`rkey` → URI, `record.record` → post body), (3) fix logger calls for project Logger API.

### 5. Tests
- No pytest tests added in this iteration; plan allowed "if added". Optional follow-up: add tests for `extract_did_from_post_uri`, `load_liked_post_uris` with mocked storage, and TapRepoSync with mocked WebSocket.

## Deviations
- Tap repo is **bluesky-social/indigo** (cmd/tap), not atproto. README and plan updated. Tap run command uses subcommand: `go run ./cmd/tap run --disable-acks=true`.
- First Tap run with existing Tap DB (from prior add_repos) yielded 0 events; fresh Tap DB + connect-before-add_repos yielded 16/20.
