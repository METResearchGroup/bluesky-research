# Tap user-likes experiment — what we captured (no further Tap runs)

This document summarizes artifacts from the Tap-backed run on **2026-03-24**, without re-running Tap.

## Summary

| Stage | Status | Notes |
|--------|--------|--------|
| **Phase A** (enumerate `app.bsky.feed.like` via Tap WebSocket + `/repos/add`) | **Succeeded** | Wrote `likes.parquet` with **1,897** rows (like records whose subject is a feed post). |
| **Phase B** (hydrate liked posts via `TapRepoSync.wait_for_posts`) | **Did not complete** | First full run was interrupted mid–Phase B; a later `--phase-b-only` retry failed with a WebSocket disconnect before `liked_posts.parquet` was written. |
| **`liked_posts.parquet`** | **Not produced** | No post bodies were persisted in this folder. |

Operational context: the machine later hit **disk full** (Tap’s local SQLite under `indigo/tap.db` grew large). We are **not** re-running Tap per team decision.

## Artifacts in this folder

| File | Description |
|------|-------------|
| `likes.parquet` | One row per like record: `user_did`, `like_uri`, `liked_post_uri`. |
| `run.log` | Stdout from the Phase A run that produced `likes.parquet`. |
| `run_phase_b.log` | Log from a Phase B–only attempt; ends in `ConnectionClosedError` from the Tap WebSocket client. |

## Phase A numbers (from `run.log`)

- Seed: **4** DIDs from `user_dids.json` (see repo `experiments/tap_experiments_hydrate_likes_2026_03_24/user_dids.json`).
- **1,897** distinct `like_uri` keys collected (deduped in the client).
- **1,888** distinct `liked_post_uri` values (some likes point at the same post).

## Important caveat: rows vs seed DIDs

`likes.parquet` has **1,897** rows, but only **795** of those rows have `user_did` in the **four seed** DIDs. The remaining **1,102** rows are likes whose **liker repo** (`user_did`) is **not** in the seed list.

That usually means the Tap instance was also delivering events for **other tracked repos** (e.g. leftover state in Tap’s DB from earlier `/repos/add` activity), while the WebSocket stream is **not** filtered to “only these four DIDs” in our client—we record every `app.bsky.feed.like` post-like we parse.

**Counts for the four seed DIDs only** (rows in `likes.parquet`):

| `user_did` | Rows (like records) |
|------------|----------------------|
| `did:plc:w5mjarupsl6ihdrzwgnzdh4y` | 398 |
| `did:plc:e4itbqoxctxwrrfqgs2rauga` | 206 |
| `did:plc:fbnm4hjnzu4qwg3nfjfkdhay` | 191 |
| `did:plc:gedsnv7yxi45a4g2gts37vyp` | **0** (no rows in this capture) |

Distinct liked posts **among seed rows only**: **794** `liked_post_uri` values.

For “official” seed-only analysis, filter Parquet with `user_did` ∈ seed list (or use a fresh Tap DB and only `/repos/add` the four DIDs before a future run).

## Phase B (what failed)

- Intended: add **1,196** poster DIDs and stream until **1,888** post records arrive (or timeout).
- Observed: WebSocket closed with `websockets.exceptions.ConnectionClosedError: no close frame received or sent` during `wait_for_posts` (see `run_phase_b.log`). **No** `liked_posts.parquet` was written.

## Per-person report (all rows in `likes.parquet`)

From `report_likes_per_user.py` (counts **like records** per `user_did`, not distinct posts):

```
                        user_did  posts_liked
did:plc:cohsnro2uub7ol542ee2qaig          746
did:plc:w5mjarupsl6ihdrzwgnzdh4y          398
did:plc:474ldquxwzrlcvjhhbbk2wte          277
did:plc:e4itbqoxctxwrrfqgs2rauga          206
did:plc:fbnm4hjnzu4qwg3nfjfkdhay          191
… (7 more DIDs with smaller counts)
Total like records: 1897
```

For **seed-only** per-person counts, filter as above; the helper script can be pointed at a filtered Parquet export if you create one.

## Next steps (without Tap)

- Use **`likes.parquet`** for downstream analysis (engagement, graph, etc.).
- Hydrate posts **outside Tap** if needed (e.g. PDS `getRecord` / app API) using `liked_post_uri`—only if you accept separate rate limits and semantics.
- If you ever retry Tap: start from a **clean Tap DB**, **filter client-side to seed DIDs**, and consider **batching** Phase B poster DIDs to avoid huge single-session WebSocket loads.
