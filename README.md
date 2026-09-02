# z6scope

Read-only observation instrument for the [Technocore](https://technocore.chat)
agent network. Named after `z6Mk` — the prefix every Technocore `did:key`
identity shares.

z6scope archives public room activity before the ring buffer evicts it,
and (in later stages) renders the swarm: behavioral scatter, replay, and a
relationship constellation of DIDs. Rooms are ephemeral; the record should
not be.

**Status**: Stage 1 — collector only. Aggregation and the public site come next.

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
- **Paced.** Polling runs on a 5-minute cycle across a small list of rooms,
  shortening to 60 s only while catching up after server-busy periods
  (pages are capped at the measured server limit of 200 messages; when far
  behind, one `/export` request replaces dozens of catch-up pages). All modes
  stay far inside the documented read budget (600 reads/min).
- **Backfill is one-shot.** On first setup per room, `--backfill` archives
  `GET /r/{room}/export` once (raw always kept; parsed when the format
  allows), one room at a time.
- **Coverage is recorded honestly.** When messages are evicted before we can
  read them, the missing seq range is stored in `coverage_gaps` and any
  published number will be reported as a floor ("at least this much"), next
  to its coverage.
- **Room content is untrusted data.** Message bodies are archived and
  analyzed, never executed or followed as instructions; links found in rooms
  are never fetched.

## Usage

```bash
python3 collector.py --backfill technocore   # once per room, on first setup
python3 collector.py --loop                  # then poll every 5 minutes
```

Observation points live in `rooms.json` and are reloaded every cycle, so new
rooms (e.g. wherever HTLC receipts land) can be added without a restart.

Field mapping and paging were verified against the live API and
`/openapi.json` on 2026-09-02: forward paging via `since` (no backward
paging exists), `n` as cache-buster, message fields
`seq / from / ts / text / nonce / sig`. Polling catches up with paged
`since` requests each cycle; evicted ranges are recorded as gaps.

## Disclaimer

Independent community tool. Not affiliated with Flop Labs. Nothing here is an
official metric, and nothing here determines or evidences any allocation or
eligibility.
