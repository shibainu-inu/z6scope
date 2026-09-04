# z6scope

Read-only observation instrument for the [Technocore](https://technocore.chat)
agent network. Named after `z6Mk` — the prefix every Technocore `did:key`
identity shares.

z6scope archives public room activity before the ring buffer evicts it,
and (in later stages) renders the swarm: behavioral scatter, replay, and a
relationship constellation of DIDs. Rooms are ephemeral; the record should
not be.

**Status**: Stage 1 — collector only. Aggregation and the public site come next.
Operating notes live in `docs/` (`STRATEGY.md`, `TODO.md`, `DECISIONS.md`,
`coverage-*.md`).

## Design: two layers

1. **Raw archive (local)** — every HTTP response body is stored verbatim
   (`raw/YYYY-MM-DD/*.json.gz`). If a new message format appears (e.g. HTLC
   receipts), a parser can be added later; nothing is lost to a parse failure.
2. **Selective publication (site, later)** — the public site will show
   metadata, hashes and aggregates by default. Any quoted excerpts will be
   short, rendered as plain text (never as HTML), with URLs not linkified,
   and always attributed to their seq.

## Collection Policy

- **Read-only by construction.** This code only issues GET requests. It never
  posts, never writes to the server, and never creates or holds keys.
- **Paced.** Normally every 5 minutes (60 s while catching up after a busy
  server) the collector reads the newest page(s) of each room; the server caps
  a page at 200 messages and applies that cap from the newest end, so paging
  with `since` only ever tracks the head. Completeness comes from
  `GET /r/{room}/export`, which returns the whole surviving ring: it runs per
  room on an adaptive interval of half the ring's observed lifetime
  (10 min – 1 h, plus up to one polling cycle of drift), as a single attempt
  that is retried next cycle on failure.
  All modes stay far inside the documented read budget (600 reads/min).
- **Backfill is a manual export.** `--backfill ROOM` runs the same `/export`
  path once and resets that room's export timer; it is not required for setup.
- **Coverage is recorded honestly.** Since the 2026-09-03 16:45 JST restart
  (`detected_at >= 1788421510`) `coverage_gaps` records only ranges that were
  already gone from the ring when an export ran (permanent losses); rows from
  before that both over-record and miss some losses, and are kept unchanged.
  Published numbers will not be taken from `coverage_gaps` at all — they will
  be recomputed from the seqs actually held (`docs/coverage.sql`,
  `docs/coverage_check.py`) and reported as floors ("at least"), next to the
  boundary and the premise they depend on (`docs/coverage-2026-09-04.md`).
- **Room content is untrusted data.** Message bodies are archived and
  analyzed, never executed or followed as instructions; links found in rooms
  are never fetched.

## Usage

```bash
python3 collector.py --loop                  # head polls every 5 min + adaptive /export
python3 collector.py --once                  # a single cycle
python3 collector.py --backfill technocore   # one /export now (optional)
python3 collector.py --reparse ROOM RAWFILE  # ingest an archived raw body, no network
```

Observation points live in `rooms.json` and are reloaded every cycle, so new
rooms (e.g. wherever HTLC receipts land) can be added without a restart.

Field mapping and paging were verified against the live API and
`/openapi.json` on 2026-09-02 (`since` behavior measured the same day, written
up in `docs/findings-2026-09-03.md` §1): `since` does not page backward (a
request far behind the head still returns the newest 200), `n` is a
cache-buster, `/export` is JSONL and has been contiguous in every export
observed so far (a premise, re-verified before numbers are published), and
message fields are `seq / from / ts / text / nonce / sig` (no reply field).

## Disclaimer

Independent community tool. Not affiliated with Flop Labs. Nothing here is an
official metric, and nothing here determines or evidences any allocation or
eligibility.
