#!/usr/bin/env python3
"""z6scope collector — read-only archiver for technocore.chat rooms.

Two-layer design (raw archive + selective publication):
  layer 1: every HTTP response body is stored verbatim (gzip) under raw/
  layer 2: parsed fields go into SQLite (messages / coverage / meta)
The publication layer (site JSON) is a separate tool and is NOT here.

Read-only by construction: this program only issues GET requests.
It never posts, never writes to the server, never creates keys.

API facts verified against /openapi.json (2026-09-02):
  - GET /r/{room}?format=json&limit=N&since=SEQ  (forward paging only;
    `since` returns messages with a GREATER seq — there is no backward
    paging parameter)
  - `n` query param: ignored by the server, varies the URL past a cache
    (used here as a cache-buster on every request)
  - GET /r/{room}/export exists (used for --backfill; format verified at
    first run — raw is archived even if parsing is not yet wired)

Usage:
  python3 collector.py --once            # one polling cycle over all rooms
  python3 collector.py --loop            # poll every INTERVAL_SEC forever
  python3 collector.py --backfill lobby  # one-shot: archive /export of a room
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import random
import re
import signal
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# ----------------------------------------------------------------------------
# configuration
# ----------------------------------------------------------------------------
BASE_URL = "https://technocore.chat"
ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "rooms.json"       # observation points live in config,
                                        # not in code (HTLC rooms get added here)
DB_PATH = ROOT / "data" / "z6scope.sqlite3"
RAW_DIR = ROOT / "raw"

INTERVAL_SEC = 300          # normal polling cycle (5-min discipline)
FAST_RETRY_SEC = 60         # shortened cycle while behind (busy/page-capped);
                            # /r/technocore's ring turns over in ~77 min, so
                            # debt must be repaid quickly or it is evicted
PAGE_LIMIT = 200            # server caps a page at 200 (measured 2026-09-02:
                            # limit=1000 and limit=5000 both returned count=200)
MAX_PAGES_PER_CYCLE = 40    # safety cap per room per cycle (8,000 msgs)
EXPORT_INTERVAL_SEC = 3600  # UPPER bound for the per-room /export interval —
                            # the ONLY complete source (see poll_room). Rings
                            # are byte-capped (~10 MiB), so their lifetime
                            # swings with rate: 1–3 h on 2026-09-02, but only
                            # 34–65 min for technocore/kibble on the evening
                            # of 2026-09-03, when a fixed hour lost ~19k seqs.
EXPORT_MIN_SEC = 600        # lower bound — politeness floor even for a room
                            # whose ring turns over in minutes
EXPORT_LIFETIME_TRIM = 0.01     # drop this fraction of ts at each end before
                            # measuring the ring lifetime (outlier guard)
EXPORT_LIFETIME_FRACTION = 0.5  # interval = ring lifetime × this. The timer
                            # is checked once per cycle per room, so real
                            # spacing = interval + cycle drift (INTERVAL_SEC
                            # plus that cycle's work, up to a few minutes).
                            # Halving leaves ~1.5× headroom for a further
                            # rate rise at a 34-min ring; once the 600 s
                            # floor binds (lifetime < ~13 min with drift)
                            # loss resumes and is recorded honestly.
PAGE_SLEEP_SEC = 0.5        # pause between catch-up pages
POLL_SLEEP_BETWEEN_ROOMS = 2.0
HTTP_TIMEOUT = 20
EXPORT_TIMEOUT = 90   # /export generates the whole ring server-side; be patient
RETRIES = 5
RETRY_SLEEP = 5.0
RETRY_MAX_WAIT = 60.0        # cap for a single backoff wait
BUSY_COOLDOWN_SEC = 15       # extra pause after giving up on a busy server
USER_AGENT = "z6scope-collector/0.2 (read-only archiver; github.com/shibainu-inu/z6scope)"

# --- FIELD MAPPING (verified against live /r/technocore, 2026-09-02) --------
KEYS_SEQ = ("seq",)
KEYS_DID = ("from", "did", "sender")
KEYS_TS = ("ts", "time", "timestamp")
KEYS_BODY = ("text", "body", "message")
KEYS_NONCE = ("nonce",)
KEYS_SIG = ("sig",)
MESSAGES_KEY_CANDIDATES = ("messages", "items", "msgs")

RE_REPLY = re.compile(r"^\s*re:\s*(\d+)", re.IGNORECASE)

log = logging.getLogger("z6scope")

# ----------------------------------------------------------------------------
# storage
# ----------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  room      TEXT NOT NULL,
  seq       INTEGER NOT NULL,
  did       TEXT,
  ts        TEXT,
  body      TEXT,
  body_hash TEXT,
  body_len  INTEGER,
  reply_to  INTEGER,
  nonce     TEXT,
  sig       TEXT,
  fetched_at INTEGER NOT NULL,
  PRIMARY KEY (room, seq)
);
CREATE INDEX IF NOT EXISTS idx_messages_did ON messages(did);
CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages(room, ts);

-- seq ranges we know we can never recover (evicted from the ring buffer
-- before we could read them). Honest coverage accounting.
CREATE TABLE IF NOT EXISTS coverage_gaps (
  room       TEXT NOT NULL,
  gap_start  INTEGER NOT NULL,
  gap_end    INTEGER NOT NULL,
  detected_at INTEGER NOT NULL,
  PRIMARY KEY (room, gap_start)
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS parse_failures (
  room       TEXT NOT NULL,
  fetched_at INTEGER NOT NULL,
  raw_file   TEXT,
  reason     TEXT
);
"""

MIGRATIONS = (
    "ALTER TABLE messages ADD COLUMN nonce TEXT",
    "ALTER TABLE messages ADD COLUMN sig TEXT",
)


def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    for stmt in MIGRATIONS:          # no-ops on a fresh DB, safe on an old one
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass
    con.commit()
    return con


def meta_get(con: sqlite3.Connection, key: str) -> str | None:
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def meta_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?,?)", (key, value))
    con.commit()


# ----------------------------------------------------------------------------
# fetch (GET only)
# ----------------------------------------------------------------------------
def http_get(url: str, timeout: int = HTTP_TIMEOUT,
             retries: int = RETRIES) -> bytes:
    """GET with polite backoff. `retries=1` is for the slow /export path so a
    stalled export cannot block the whole cycle for ~8 min (5 × 90 s)."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 503):
                retry_after = 0.0
                try:
                    retry_after = float(e.headers.get("Retry-After", "0"))
                except (TypeError, ValueError):
                    pass
                backoff = min(RETRY_SLEEP * (2 ** (attempt - 1)), RETRY_MAX_WAIT)
                wait = max(retry_after, backoff) + random.uniform(0, 2)
                log.warning("HTTP %s on %s — backing off %.1fs (attempt %d/%d)",
                            e.code, url, wait, attempt, retries)
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            log.warning("network error on %s (attempt %d/%d): %s", url, attempt, retries, e)
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"giving up on {url}: {last_err}")


def room_url(room: str, since: int | None = None) -> str:
    params = {"format": "json", "limit": str(PAGE_LIMIT), "n": str(int(time.time() * 1000))}
    if since is not None:
        params["since"] = str(since)
    return f"{BASE_URL}/r/{urllib.parse.quote(room)}?{urllib.parse.urlencode(params)}"


def export_url(room: str) -> str:
    return f"{BASE_URL}/r/{urllib.parse.quote(room)}/export"


def save_raw(room: str, payload: bytes, fetched_at: int, kind: str = "poll") -> str:
    day = time.strftime("%Y-%m-%d", time.gmtime(fetched_at))
    d = RAW_DIR / day
    d.mkdir(parents=True, exist_ok=True)
    name = f"{room.replace('/', '_')}-{kind}-{fetched_at}-{int(time.monotonic()*1000)%100000}.json.gz"
    path = d / name
    with gzip.open(path, "wb") as f:
        f.write(payload)
    return str(path.relative_to(ROOT))


# ----------------------------------------------------------------------------
# parse — untrusted data: bytes in, plain values out, nothing executed
# ----------------------------------------------------------------------------
def first_key(d: dict, keys: tuple) -> object:
    for k in keys:
        if k in d:
            return d[k]
    return None


def extract(payload: bytes) -> tuple[dict, list[dict]]:
    """Returns (top_level_doc, message_dicts).

    Handles both response shapes observed live (2026-09-02):
      - room polling: one JSON object with a "messages" array
      - /export: JSONL — one JSON object per line ("Extra data" on whole-body
        parse is the signature of this format)
    """
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError as e:
        if "Extra data" not in str(e):
            raise
        return ({}, _extract_jsonl(payload))
    if isinstance(doc, list):
        return ({}, [m for m in doc if isinstance(m, dict)])
    if isinstance(doc, dict):
        for k in MESSAGES_KEY_CANDIDATES:
            v = doc.get(k)
            if isinstance(v, list):
                return (doc, [m for m in v if isinstance(m, dict)])
    raise ValueError("no message array found (adjust MESSAGES_KEY_CANDIDATES)")


def _extract_jsonl(payload: bytes) -> list[dict]:
    msgs: list[dict] = []
    bad = 0
    for line in payload.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            bad += 1
            continue
        if not isinstance(obj, dict):
            continue
        picked = False
        for k in MESSAGES_KEY_CANDIDATES:
            v = obj.get(k)
            if isinstance(v, list):
                msgs.extend(m for m in v if isinstance(m, dict))
                picked = True
                break
        if not picked:
            msgs.append(obj)   # the line itself is a message object
    if bad:
        log.warning("jsonl: %d unparseable lines skipped (raw retains them)", bad)
    if not msgs:
        raise ValueError("jsonl detected but no message objects found")
    return msgs


def normalize(msg: dict) -> dict | None:
    seq = first_key(msg, KEYS_SEQ)
    if seq is None:
        return None
    body = first_key(msg, KEYS_BODY)
    body = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
    m = RE_REPLY.match(body or "")
    nonce = first_key(msg, KEYS_NONCE)
    return {
        "seq": int(seq),
        "did": first_key(msg, KEYS_DID),
        "ts": str(first_key(msg, KEYS_TS)),
        "body": body,
        "body_hash": hashlib.sha256((body or "").encode("utf-8")).hexdigest(),
        "body_len": len(body or ""),
        "reply_to": int(m.group(1)) if m else None,
        "nonce": None if nonce is None else str(nonce),
        "sig": first_key(msg, KEYS_SIG),
    }


# ----------------------------------------------------------------------------
# ingest
# ----------------------------------------------------------------------------
def ingest(con: sqlite3.Connection, room: str, payload: bytes,
           fetched_at: int, raw_file: str) -> dict:
    """Returns {'parsed','inserted','min_seq','max_seq','first_seq','last_seq'}
    (seq fields are -1 when absent)."""
    out = {"parsed": 0, "inserted": 0, "min_seq": -1, "max_seq": -1,
           "first_seq": -1, "last_seq": -1}
    try:
        doc, raw_msgs = extract(payload)
    except (ValueError, json.JSONDecodeError) as e:
        con.execute(
            "INSERT INTO parse_failures(room, fetched_at, raw_file, reason) VALUES (?,?,?,?)",
            (room, fetched_at, raw_file, str(e)))
        con.commit()
        log.error("[%s] parse failure (%s) — raw kept at %s", room, e, raw_file)
        return out

    if isinstance(doc.get("first_seq"), int):
        out["first_seq"] = doc["first_seq"]
    if isinstance(doc.get("last_seq"), int):
        out["last_seq"] = doc["last_seq"]

    rows = []
    for m in raw_msgs:
        n = normalize(m)
        if n is None:
            continue
        out["min_seq"] = n["seq"] if out["min_seq"] < 0 else min(out["min_seq"], n["seq"])
        out["max_seq"] = max(out["max_seq"], n["seq"])
        rows.append((room, n["seq"], n["did"], n["ts"], n["body"], n["body_hash"],
                     n["body_len"], n["reply_to"], n["nonce"], n["sig"], fetched_at))
    cur = con.executemany(
        "INSERT OR IGNORE INTO messages"
        "(room, seq, did, ts, body, body_hash, body_len, reply_to, nonce, sig, fetched_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    out["parsed"], out["inserted"] = len(rows), cur.rowcount
    return out


def record_gap(con: sqlite3.Connection, room: str, start: int, end: int) -> None:
    if end < start:
        return
    con.execute(
        "INSERT OR IGNORE INTO coverage_gaps(room, gap_start, gap_end, detected_at)"
        " VALUES (?,?,?,?)", (room, start, end, int(time.time())))
    con.commit()
    # Only /export calls this: a range the ring no longer holds is gone for
    # good. Ranges that polling merely skips are not gaps (see poll_room).
    log.warning("[%s] coverage gap recorded: seq %d..%d "
                "(not in ring at export time — permanently lost)",
                room, start, end)


# ----------------------------------------------------------------------------
# modes
# ----------------------------------------------------------------------------
def stored_max_seq(con: sqlite3.Connection, room: str) -> int | None:
    row = con.execute("SELECT MAX(seq) FROM messages WHERE room=?", (room,)).fetchone()
    return row[0]


def poll_room(con: sqlite3.Connection, room: str) -> bool:
    """Poll the head of the room, plus a periodic /export for completeness.

    `since` does NOT page backward (measured 2026-09-02): the 200-message cap
    is applied from the NEWEST end, so `since=<newest-5000>` returns the newest
    200 — not the 200 following the cursor. Polling therefore only keeps up
    while we are within one page of the head; anything further back is skipped
    by the server, not evicted from the ring. /export (the whole surviving
    ring, JSONL, contiguous) is the only complete source, so it runs on a timer
    rather than on a lag estimate.

    Gap rule: when the first page's oldest seq is > cursor+1, that range was
    not read in this cycle and is recorded as a coverage gap. A later /export
    usually fills it; the record is kept regardless (rule 5).
    """
    maybe_periodic_export(con, room)
    cursor = stored_max_seq(con, room)
    total_new = 0
    busy = False
    for page in range(MAX_PAGES_PER_CYCLE):
        fetched_at = int(time.time())
        try:
            payload = http_get(room_url(room, since=cursor))
        except (RuntimeError, urllib.error.HTTPError) as e:
            # Politeness over completeness: stop pushing a stressed server.
            # Nothing is lost yet — the cursor stays at the last stored seq
            # and the next cycle resumes from it via `since`.
            log.warning("[%s] server busy after retries (%s) — "
                        "yielding, will resume next cycle from seq %s",
                        room, e, cursor)
            busy = True
            break
        raw_file = save_raw(room, payload, fetched_at)
        r = ingest(con, room, payload, fetched_at, raw_file)
        if r["parsed"] == 0:
            break
        if page == 0:
            if cursor is None:
                meta_set(con, f"first_observed:{room}", str(r["min_seq"]))
                log.info("[%s] observation starts at seq %d "
                         "(older history is unreachable via polling; use --backfill)",
                         room, r["min_seq"])
            elif r["min_seq"] > cursor + 1:
                # Expected on busy rooms: the server handed us the head window,
                # not the page after the cursor. Not a gap — the periodic export
                # fills it; only export-confirmed losses are recorded.
                log.info("[%s] head window starts at %d (%d seqs behind cursor; "
                         "export fills)", room, r["min_seq"],
                         r["min_seq"] - cursor - 1)
        total_new += r["inserted"]
        cursor = r["max_seq"]
        if r["parsed"] < PAGE_LIMIT:
            break  # caught up with the head window
        snooze(PAGE_SLEEP_SEC)
    else:
        log.warning("[%s] page cap (%d) hit — fast retry next cycle",
                    room, MAX_PAGES_PER_CYCLE)
        busy = True
    log.info("[%s] new=%d cursor=%s%s", room, total_new, cursor,
             " (behind, fast retry)" if busy else "")
    return busy


def record_losses_below(con: sqlite3.Connection, room: str, ring_min: int) -> None:
    """Every seq below the ring's oldest that we do not hold is gone for good.

    Scans [floor, ring_min-1] for holes, where floor is the previous export's
    max (everything below it was already judged) or, on the first export under
    this regime, our oldest stored seq. Head polling cannot be used as the
    reference because it keeps the cursor at the head regardless of holes.
    """
    prev = meta_get(con, f"last_export_max:{room}")
    if prev is not None:
        floor = int(prev) + 1
    else:
        row = con.execute("SELECT MIN(seq) FROM messages WHERE room=?", (room,)).fetchone()
        if row[0] is None:
            return
        floor = row[0]
    if floor > ring_min - 1:
        return
    expected = floor
    for (s,) in con.execute("SELECT seq FROM messages WHERE room=? AND seq BETWEEN ? AND ?"
                            " ORDER BY seq", (room, floor, ring_min - 1)):
        if s > expected:
            record_gap(con, room, expected, s - 1)
        expected = s + 1
    if expected <= ring_min - 1:
        record_gap(con, room, expected, ring_min - 1)


def adapt_export_interval(con: sqlite3.Connection, room: str,
                          min_seq: int, max_seq: int) -> None:
    """Set the room's export interval to a fraction of the ring's observed
    lifetime (1%–99% trimmed ts span of what the export returned), clamped
    to [EXPORT_MIN_SEC, EXPORT_INTERVAL_SEC]. SQLite parses the server's
    `…Z` timestamps; Python 3.10's fromisoformat does not. A ring whose span
    cannot be measured (bad ts, single message) keeps the previous value."""
    # Trimmed span, not MAX-MIN: on 2026-09-04 a single message whose ts was
    # 4.5 h older than its neighbours inflated a 43-min ring to "315 min", the
    # interval jumped to the 1 h cap and 7,146 seqs were lost. ts is not
    # strictly monotonic in seq either. Dropping the lowest and highest 1%
    # (k rows each side; k = 0 below 100 rows) makes a handful of outliers
    # irrelevant. Limits, deliberately accepted: more than 1% of rows skewed
    # in one direction still inflates the span → cap → loss, and the only
    # signal is this function's log line; the guard is weakest on small rings
    # (k = 0 below 100 rows), which sit at the cap anyway. Trimming can only
    # shorten the span, never lengthen it. julianday() returns NULL for
    # unparseable ts; those rows are excluded before ranking. ORDER BY ts
    # ranks TEXT, which is chronological because the server's ts is
    # fixed-width `…THH:MM:SS.ffffffZ` (verified over all rows 2026-09-04);
    # a stray other format could only shorten the span (both endpoints are
    # members of the set), never inflate it.
    n = con.execute("SELECT COUNT(*) FROM messages WHERE room=? AND seq BETWEEN ? AND ?"
                    " AND julianday(ts) IS NOT NULL", (room, min_seq, max_seq)).fetchone()[0]
    k = int(n * EXPORT_LIFETIME_TRIM)
    q = ("SELECT julianday(ts) FROM messages WHERE room=? AND seq BETWEEN ? AND ?"
         " AND julianday(ts) IS NOT NULL ORDER BY ts %s LIMIT 1 OFFSET ?")
    lo = con.execute(q % "ASC", (room, min_seq, max_seq, k)).fetchone()
    hi = con.execute(q % "DESC", (room, min_seq, max_seq, k)).fetchone()
    span = None if lo is None or hi is None else (hi[0] - lo[0]) * 86400
    if span is None or span <= 0:
        log.warning("[%s] ring lifetime not measurable (no parseable ts, single "
                    "message, or non-positive trimmed span) — export interval "
                    "unchanged", room)
        return
    lifetime = float(span)
    interval = int(min(EXPORT_INTERVAL_SEC,
                       max(EXPORT_MIN_SEC, lifetime * EXPORT_LIFETIME_FRACTION)))
    meta_set(con, f"export_interval:{room}", str(interval))
    log.info("[%s] ring lifetime (1–99%% trimmed) %.0fm (%ds) → next export in %dm (%ds)",
             room, lifetime / 60, int(lifetime), interval // 60, interval)


def recover_via_export(con: sqlite3.Connection, room: str) -> dict:
    """Fetch the whole surviving ring via /export and ingest it.

    Raw body is archived before parsing. On success, holes below the ring's
    oldest seq are recorded as permanent losses and the scan floor advances.
    """
    fetched_at = int(time.time())
    payload = http_get(export_url(room), timeout=EXPORT_TIMEOUT, retries=1)
    raw_file = save_raw(room, payload, fetched_at, kind="export")
    r = ingest(con, room, payload, fetched_at, raw_file)
    if r["parsed"]:
        record_losses_below(con, room, r["min_seq"])
        meta_set(con, f"last_export_max:{room}", str(r["max_seq"]))
        log.info("[%s] export: parsed=%d new=%d seq %d..%d",
                 room, r["parsed"], r["inserted"], r["min_seq"], r["max_seq"])
        adapt_export_interval(con, room, r["min_seq"], r["max_seq"])
    else:
        log.warning("[%s] export not parsed — raw archived at %s", room, raw_file)
    return r


def maybe_periodic_export(con: sqlite3.Connection, room: str) -> None:
    """One /export per room per its adaptive interval (see
    adapt_export_interval; EXPORT_INTERVAL_SEC until the first export).

    This is what actually makes coverage complete: head polling alone loses
    everything beyond the newest page between cycles, because `since` cannot
    page backward. The timer is advanced only on success, so a failed or
    timed-out export is retried next cycle; the raw body is archived before
    parsing either way, so a crash here never loses data.
    """
    last = meta_get(con, f"last_export:{room}")
    interval = float(meta_get(con, f"export_interval:{room}") or EXPORT_INTERVAL_SEC)
    if last is not None and time.time() - float(last) < interval:
        return
    try:
        r = recover_via_export(con, room)
    except (RuntimeError, urllib.error.HTTPError, OSError, ValueError) as e:
        log.warning("[%s] periodic export failed (%s) — retrying next cycle",
                    room, e)
        return
    if r["parsed"]:
        meta_set(con, f"last_export:{room}", str(int(time.time())))


def backfill_room(con: sqlite3.Connection, room: str) -> None:
    """One-shot /export, same path as the periodic one so the loss scan and
    the export timer stay consistent (a manual backfill counts as an export)."""
    r = recover_via_export(con, room)
    if r["parsed"]:
        meta_set(con, f"last_export:{room}", str(int(time.time())))


def reparse_raw(con: sqlite3.Connection, room: str, raw_path: str) -> None:
    """Ingest an already-archived raw file without touching the server."""
    path = (ROOT / raw_path) if not Path(raw_path).is_absolute() else Path(raw_path)
    with gzip.open(path, "rb") as f:
        payload = f.read()
    fetched_at = int(time.time())
    r = ingest(con, room, payload, fetched_at, str(raw_path))
    log.info("[%s] reparse %s: parsed=%d new=%d seq %d..%d",
             room, path.name, r["parsed"], r["inserted"], r["min_seq"], r["max_seq"])


def load_rooms() -> list[str]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    rooms = cfg.get("rooms", [])
    if not rooms:
        raise SystemExit("rooms.json has no rooms")
    return rooms


_stop = False


def _sig_handler(_n, _f):
    global _stop
    _stop = True


def snooze(seconds: float) -> None:
    """Sleep that reacts to SIGTERM/Ctrl+C within ~1 s."""
    end = time.monotonic() + seconds
    while not _stop:
        left = end - time.monotonic()
        if left <= 0:
            return
        time.sleep(min(1.0, left))


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--once", action="store_true")
    g.add_argument("--loop", action="store_true")
    g.add_argument("--backfill", metavar="ROOM")
    g.add_argument("--reparse", nargs=2, metavar=("ROOM", "RAWFILE"))
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, _sig_handler)   # SIGINT (Ctrl+C) stays
    con = open_db()                               # default: instant stop

    if args.backfill:
        backfill_room(con, args.backfill)
        return
    if args.reparse:
        reparse_raw(con, args.reparse[0], args.reparse[1])
        return

    try:
        while True:
            any_behind = False
            for room in load_rooms():  # reloaded every cycle: HTLC rooms can
                if _stop:              # be added without a restart
                    return
                busy = False
                try:
                    busy = poll_room(con, room)
                except Exception as e:  # noqa: BLE001 — keep the loop alive
                    log.error("[%s] poll failed: %s", room, e)
                    busy = True
                if busy:
                    any_behind = True
                    snooze(BUSY_COOLDOWN_SEC)
                snooze(POLL_SLEEP_BETWEEN_ROOMS)
            if args.once or _stop:
                return
            snooze(FAST_RETRY_SEC if any_behind else INTERVAL_SEC)
    except KeyboardInterrupt:
        log.info("interrupted — cursor is durable, resuming is safe")


if __name__ == "__main__":
    main()
